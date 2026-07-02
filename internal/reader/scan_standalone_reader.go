package reader

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

type ScanReaderOptions struct {
	Cluster         bool             `mapstructure:"cluster" default:"false"`
	Address         string           `mapstructure:"address" default:""`
	Username        string           `mapstructure:"username" default:""`
	Password        string           `mapstructure:"password" default:""`
	Tls             bool             `mapstructure:"tls" default:"false"`
	TlsConfig       client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	Scan            bool             `mapstructure:"scan" default:"true"`
	KSN             bool             `mapstructure:"ksn" default:"false"`
	DBS             []int            `mapstructure:"dbs"`
	PreferReplica   bool             `mapstructure:"prefer_replica" default:"false"`
	Count           int              `mapstructure:"count" default:"1"`
	SkipUnknownType []string         `mapstructure:"skip_unknown_type" default:"[]"`
	// DumpParallel is the number of independent source connections that drain
	// the shared needDumpQueue and run DUMP+PTTL. A single connection caps at
	// ~22k keys/s (round-trip latency), starving downstream writers. Each worker
	// pulls keys off the shared channel (no key partitioning needed). Default 1.
	DumpParallel int `mapstructure:"dump_parallel" default:"1"`
	// DumpQueueSize bounds the scan()->dump() handoff queue. scan() enumerates
	// key names far faster than dump() processes them; an unbounded queue buffers
	// ~the whole keyspace in RAM (OOM on large DBs) and the large live heap also
	// degrades throughput via GC. The bounded default makes Put() block,
	// backpressuring scan() to dump() throughput so memory stays flat. Set a
	// large value to restore the previous effectively-unbounded behavior.
	DumpQueueSize int `mapstructure:"dump_queue_size" default:"100000"`
	// BigKeys are key names that must NOT be moved via DUMP/RESTORE. DUMP serializes
	// the whole value into one reply; for a huge collection (e.g. a 120M-member zset)
	// that is a multi-GB bulk that blows proto-max-bulk-len and can OOM-crash the
	// source. Listed keys are instead read incrementally with a type-native cursor
	// (ZSCAN/HSCAN/SSCAN/LRANGE) and replayed as batched commands — bounded memory on
	// both ends. Assumes a fresh/flushed target (the rollback recipe flushes it), so no
	// leading DEL is emitted.
	BigKeys []string `mapstructure:"big_keys" default:"[]"`
	// StreamLists routes every list-typed key through the cursor path (LRANGE chunks
	// replayed as RPUSH) instead of DUMP/RESTORE, without having to name each key in
	// BigKeys. Motivation: on a Dragonfly->Redis rollback, producing the DUMP payload
	// for lists is the throughput bottleneck (multi-x slower than other types); reading
	// them with LRANGE bypasses that serialization. Enabled only for the SCAN phase.
	// scan() classifies each scanned key with a batched TYPE probe; lists go to a bounded
	// pool of stream workers, everything else keeps the fast pipelined DUMP path.
	StreamLists bool `mapstructure:"stream_lists" default:"false"`
	// StreamParallel is the number of pooled connections that drain the list-stream
	// queue when StreamLists is set (replaces the per-key goroutine used for BigKeys,
	// which does not scale to a keyspace-sized set of lists). Ignored unless StreamLists.
	StreamParallel int `mapstructure:"stream_parallel" default:"4"`
}

type dbKey struct {
	db  int
	key string
}

type needRestoreItem struct {
	dbId int
	key  string
}

// dumpWorker is one source connection that drains the shared needDumpQueue.
// Each worker runs its own dump()/restore() pair on its own connection, so
// pipelined replies stay ordered per-connection. Workers share needDumpQueue
// (input) and r.ch (output); the Go channel hands each key to exactly one
// worker, so no key partitioning is needed.
type dumpWorker struct {
	client          *client.Redis
	needRestoreChan chan *needRestoreItem
	isValkey        bool
}

type scanStandaloneReader struct {
	ctx             context.Context
	dbs             []int
	opts            *ScanReaderOptions
	ch              chan *entry.Entry
	needDumpQueue   *utils.UniqueQueue
	needStreamQueue *utils.UniqueQueue // list keys routed to the stream pool (StreamLists); nil when disabled
	subWG           sync.WaitGroup
	restoreWG       sync.WaitGroup
	bigKeyWG        sync.WaitGroup // per-key streamers for BigKeys (cursor-read, not DUMP)
	streamPoolWG    sync.WaitGroup // pooled stream workers draining needStreamQueue
	bigKeySeen      sync.Map       // dedup: SCAN can return a key more than once

	stat struct {
		Name              string `json:"name"`
		ScanFinished      bool   `json:"scan_finished"`
		ScanDbId          int    `json:"scan_dbId"`
		ScanCursor        uint64 `json:"scan_cursor"`
		ScanPercentByDbId string `json:"scan_percent"`
		NeedUpdateCount   int64  `json:"need_update_count"`
	}
}

func NewScanStandaloneReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	r.dbs = opts.DBS
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	queueSize := opts.DumpQueueSize
	if queueSize < 1 {
		queueSize = 100000
	}
	r.needDumpQueue = utils.NewUniqueQueue(queueSize) // bounded: backpressure scan() to dump() speed (avoids buffering whole keyspace)
	if opts.StreamLists {
		r.needStreamQueue = utils.NewUniqueQueue(queueSize) // same bound; list keys handed to the stream pool
	}
	log.Infof("[%s] scanStandaloneReader init finished. dbs=[%v], dump_queue_size=[%d], stream_lists=[%v]", r.stat.Name, r.dbs, queueSize, opts.StreamLists)
	return r
}

func (r *scanStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	if r.opts.KSN {
		r.subWG.Add(1)
		go r.subscribe()
		r.subWG.Wait()
	}
	if r.opts.Scan {
		go r.scan()
	}
	parallel := r.opts.DumpParallel
	if parallel < 1 {
		parallel = 1
	}
	log.Infof("[%s] starting %d dump worker(s)", r.stat.Name, parallel)
	for i := 0; i < parallel; i++ {
		w := &dumpWorker{
			client:          client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica),
			needRestoreChan: make(chan *needRestoreItem, 1024),
		}
		r.restoreWG.Add(1)
		go r.dump(w)
		go r.restore(w)
	}
	if r.opts.StreamLists {
		streamParallel := r.opts.StreamParallel
		if streamParallel < 1 {
			streamParallel = 1
		}
		log.Infof("[%s] starting %d list-stream worker(s)", r.stat.Name, streamParallel)
		for i := 0; i < streamParallel; i++ {
			r.streamPoolWG.Add(1)
			go r.streamWorker()
		}
	}
	// Close the output channel once every worker's restore(), every per-key big-key
	// streamer, and every pooled list-stream worker has drained. scan() finishes
	// (closing needDumpQueue) before the dump/restore pipeline drains, so all
	// bigKeyWG.Add() calls happen-before this Wait(); streamPoolWG is sized up-front.
	go func() {
		r.restoreWG.Wait()
		r.bigKeyWG.Wait()
		r.streamPoolWG.Wait()
		close(r.ch)
	}()
	return []chan *entry.Entry{r.ch}
}

func (r *scanStandaloneReader) subscribe() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	log.Infof("[%s] scanStandaloneReader subscribe started. dbs=[%v]", r.stat.Name, r.dbs)
	if len(r.dbs) == 0 {
		c.Send("psubscribe", "__keyevent@*__:*")
		_, err := c.Receive()
		if err != nil {
			log.Panicf("%v", err)
		}
	} else {
		args := []interface{}{"psubscribe"}
		for _, db := range r.dbs {
			args = append(args, fmt.Sprintf("__keyevent@%v__:*", db))
		}
		c.Send(args...)
		for range r.dbs {
			_, err := c.Receive()
			if err != nil {
				log.Panicf("%v", err)
			}
		}
	}

	// wait
	r.subWG.Done()

	regex := regexp.MustCompile(`\d+`)
	for {
		select {
		case <-r.ctx.Done():
			log.Infof("[%s] scanStandaloneReader subscribe finished.", r.stat.Name)
			r.closeInputQueues()
			return
		default:
			resp, err := c.Receive()
			if err != nil {
				log.Panicf("%v", err)
			}
			respSlice := resp.([]interface{})
			key := respSlice[3].(string)
			dbId := regex.FindString(respSlice[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf("%v", err)
			}
			// handle del action
			eventSlice := strings.Split(respSlice[2].(string), ":")
			if eventSlice[1] == "del" {
				e := entry.NewEntry()
				e.DbId = dbIdInt
				e.Argv = []string{"DEL", key}
				r.ch <- e
				continue
			}
			r.needDumpQueue.Put(dbKey{db: dbIdInt, key: key})
		}
	}
}

func (r *scanStandaloneReader) scan() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	dbs := r.dbs
	if len(r.dbs) == 0 {
		c.Send("info", "keyspace")
		info, err := c.Receive()
		if err != nil {
			log.Panicf("%v", err)
		}
		dbs = utils.ParseDBs(info.(string))
	}
	for _, dbId := range dbs {
		if dbId != 0 {
			reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
		}

		var cursor uint64 = 0
		count := r.opts.Count
		// pending buffers scanned key names for a batched TYPE probe (StreamLists only);
		// classifyAndRoute sends lists to the stream pool and the rest to the dump path.
		var pending []string
		const classifyBatch = 512
		for {
			select {
			case <-r.ctx.Done():
				log.Infof("[%s] scanStandaloneReader scan finished.", r.stat.Name)
				r.closeInputQueues()
				return
			default:
			}

			var keys []string
			cursor, keys = c.Scan(cursor, count)
			for _, key := range keys {
				if r.isBigKey(key) {
					// Stream via cursor instead of DUMP; dedup since SCAN may repeat a key.
					if _, dup := r.bigKeySeen.LoadOrStore(dbKey{dbId, key}, true); dup {
						continue
					}
					r.bigKeyWG.Add(1)
					go r.streamBigKey(dbId, key)
					continue
				}
				if r.opts.StreamLists {
					pending = append(pending, key)
					if len(pending) >= classifyBatch {
						r.classifyAndRoute(c, dbId, pending)
						pending = pending[:0]
					}
					continue
				}
				r.needDumpQueue.Put(dbKey{dbId, key}) // pass value not pointer
			}

			// stat
			r.stat.ScanCursor = cursor
			r.stat.ScanDbId = dbId
			r.stat.ScanPercentByDbId = fmt.Sprintf("%.2f%%", float64(bits.Reverse64(cursor))/float64(^uint(0))*100)

			if cursor == 0 {
				break
			}
		}
		if len(pending) > 0 { // flush the tail of this db before switching SELECT
			r.classifyAndRoute(c, dbId, pending)
			pending = nil
		}
	}
	r.stat.ScanFinished = true
	if !r.opts.KSN {
		r.closeInputQueues()
	}
}

// closeInputQueues closes the dump queue and, when list streaming is enabled, the
// stream queue — signalling the dump workers and the stream pool to drain and exit.
func (r *scanStandaloneReader) closeInputQueues() {
	r.needDumpQueue.Close()
	if r.needStreamQueue != nil {
		r.needStreamQueue.Close()
	}
}

// classifyAndRoute probes TYPE for a batch of scanned keys in one pipelined round-trip
// and routes each: list-typed keys go to the stream pool (LRANGE->RPUSH), everything
// else stays on the fast DUMP/RESTORE path. Called only when StreamLists is set.
func (r *scanStandaloneReader) classifyAndRoute(c *client.Redis, dbId int, keys []string) {
	for _, k := range keys {
		c.SendNoFlush("TYPE", k)
	}
	c.Flush()
	for _, k := range keys {
		reply, err := c.Receive()
		if err != nil {
			log.Panicf("[%s] classifyAndRoute TYPE failed key=[%s]: %v", r.stat.Name, k, err)
		}
		typ, _ := reply.(string)
		if typ == "list" {
			r.needStreamQueue.Put(dbKey{dbId, k})
		} else {
			r.needDumpQueue.Put(dbKey{dbId, k})
		}
	}
}

func (r *scanStandaloneReader) dump(w *dumpWorker) {
	nowDbId := 0
	w.isValkey = w.client.IsValkey()
	log.Infof("[%s] detected server type: %s", r.stat.Name, map[bool]string{true: "Valkey", false: "Redis"}[w.isValkey])
	// Support prefer_replica=true in both Cluster and Standalone mode
	if r.opts.PreferReplica {
		w.client.Do("READONLY")
		log.Infof("running dump() in read-only mode")
	}

	// Batch the DUMP/PTTL sends instead of flushing per command (Send() would do
	// a syscall per command — the scan reader's dominant cost). Flush every
	// `flushEvery` keys or every 5ms (whichever first) so restore() is never
	// starved and a lull can't strand a partial batch. flushEvery stays well
	// under the needRestoreChan capacity to avoid a send/receive deadlock.
	const flushEvery = 128
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	pending := 0
	for {
		select {
		case <-ticker.C:
			if pending > 0 {
				w.client.Flush()
				pending = 0
			}
			continue
		case item, ok := <-r.needDumpQueue.Ch:
			if !ok {
				if pending > 0 {
					w.client.Flush()
				}
				close(w.needRestoreChan)
				log.Infof("[%s] scanStandaloneReader dump finished.", r.stat.Name)
				return
			}
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			dbId := item.(dbKey).db
			key := item.(dbKey).key
			if nowDbId != dbId {
				w.client.SendNoFlush("SELECT", strconv.Itoa(dbId))
				nowDbId = dbId
			}
			// dump (buffered; flushed in batches below)
			w.client.SendNoFlush("DUMP", key)
			w.client.SendNoFlush("PTTL", key)
			if len(r.opts.SkipUnknownType) > 0 {
				w.client.SendNoFlush("TYPE", key)
			}
			w.needRestoreChan <- &needRestoreItem{dbId, key}
			pending++
			if pending >= flushEvery {
				w.client.Flush()
				pending = 0
			}
		}
	}
}

// restore sends RESTORE commands to the target Redis.
// Note: rdb_restore_command_behavior configuration only applies when RESTORE command is used.
// For large values exceeding target_redis_proto_max_bulk_len, individual commands (SET, HSET, etc.)
// are used instead, which may not respect the rdb_restore_command_behavior setting.
func (r *scanStandaloneReader) restore(w *dumpWorker) {
	defer r.restoreWG.Done()
	nowDbId := 0
	for item := range w.needRestoreChan {
		dbId := item.dbId
		key := item.key
		if nowDbId != dbId {
			reply, err := w.client.Receive()
			if err != nil || reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
			nowDbId = dbId
		}
		iDump, err1 := w.client.Receive()
		iPttl, err2 := w.client.Receive()
		if len(r.opts.SkipUnknownType) > 0 {
			iType, err3 := w.client.Receive()
			if err3 != nil {
				log.Panicf("%v", err3)
			}
			typeStr := iType.(string)
			// type in SkipUnknownType
			skip := false
			for _, skipType := range r.opts.SkipUnknownType {
				if strings.EqualFold(typeStr, skipType) {
					skip = true
				}
			}
			if skip {
				log.Infof("skip restore key=[%s] type=[%s]", key, typeStr)
				continue
			}
		}
		if errors.Is(err1, proto.Nil) {
			continue // key not exist
		} else if err1 != nil {
			log.Panicf("%v", err1)
		} else if err2 != nil {
			log.Panicf("%v", err2)
		}
		dump := iDump.(string)
		pttl := 0
		switch v := iPttl.(type) {
		case int64:
			pttl = int(v)
			if pttl == 0 {
				pttl = 1
			}
		case string:
			log.Panicf("iPttl is string, this should not happen. key=[%s], pttl=[%s]", key, v)
		default:
			log.Panicf("unexpected type for pttl: %T", iPttl)
		}

		if pttl == -2 {
			continue // key not exist
		}
		if pttl == -1 {
			pttl = 0 // -1 means no expire
		}
		if uint64(len(dump)) > config.Opt.Advanced.TargetRedisProtoMaxBulkLen {
			log.Warnf("key=[%s] dump len=[%d] exceeds target_redis_proto_max_bulk_len, falling back to individual commands. "+
				"rdb_restore_command_behavior setting may not work correctly for this key.", key, len(dump))
			typeByte := dump[0]
			anotherReader := strings.NewReader(dump[1 : len(dump)-10])
			o := types.ParseObject(anotherReader, typeByte, key, w.isValkey)
			cmdC := o.Rewrite()
			for cmd := range cmdC {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = cmd
				r.ch <- e
			}
			if pttl != 0 {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = []string{"PEXPIRE", key, strconv.Itoa(pttl)}
				r.ch <- e
			}
		} else {
			argv := []string{"RESTORE", key, strconv.Itoa(pttl), dump}
			if config.Opt.Advanced.RDBRestoreCommandBehavior == "rewrite" {
				argv = append(argv, "replace")
			}
			r.ch <- &entry.Entry{
				DbId: dbId,
				Argv: argv,
			}
		}
	}
	log.Infof("[%s] scanStandaloneReader restore finished.", r.stat.Name)
}

func (r *scanStandaloneReader) isBigKey(key string) bool {
	for _, k := range r.opts.BigKeys { // operator-listed, tiny — linear is fine
		if k == key {
			return true
		}
	}
	return false
}

// emitBig sends one replayed command for a streamed big key downstream.
func (r *scanStandaloneReader) emitBig(dbId int, argv []string) {
	r.ch <- &entry.Entry{DbId: dbId, Argv: argv}
}

// streamBigKey replays one oversized key (from the explicit BigKeys list) into the
// target via a type-native cursor on its own connection — NEVER DUMP, so neither the
// source nor redis-shake ever materializes the whole value. Runs concurrently with the
// small-key DUMP pipeline; gated by bigKeyWG so r.ch isn't closed before it finishes.
// Per-key goroutine model: fine for the tiny hand-listed BigKeys set (for a keyspace-
// sized set of lists use StreamLists + the pooled streamWorker instead).
// Assumes a fresh target (no leading DEL).
func (r *scanStandaloneReader) streamBigKey(dbId int, key string) {
	defer r.bigKeyWG.Done()
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	if dbId != 0 {
		if reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId)); reply != "OK" {
			log.Panicf("[%s] streamBigKey SELECT failed db=[%d]", r.stat.Name, dbId)
		}
	}
	log.Infof("[%s] streaming big key=[%s] via cursor (no DUMP)", r.stat.Name, key)
	r.replayKeyViaCursor(c, dbId, key)
}

// streamWorker is one pooled connection draining needStreamQueue (list keys routed by
// classifyAndRoute when StreamLists is set). Unlike streamBigKey it reuses a single
// connection across many keys, SELECTing only on db change, so the connection count is
// bounded by StreamParallel regardless of how many lists the keyspace holds.
func (r *scanStandaloneReader) streamWorker() {
	defer r.streamPoolWG.Done()
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	nowDbId := 0
	for item := range r.needStreamQueue.Ch {
		dbId := item.(dbKey).db
		key := item.(dbKey).key
		if nowDbId != dbId {
			if reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId)); reply != "OK" {
				log.Panicf("[%s] streamWorker SELECT failed db=[%d]", r.stat.Name, dbId)
			}
			nowDbId = dbId
		}
		// Leading DEL for idempotency: UniqueQueue only dedups pending items, so a key
		// re-emitted by SCAN after it was already streamed would otherwise append a
		// second copy. DEL is a no-op on the fresh/flushed rollback target for a key
		// seen once, and rebuilds cleanly on a re-stream.
		r.emitBig(dbId, []string{"DEL", key})
		r.replayKeyViaCursor(c, dbId, key)
	}
	log.Infof("[%s] scanStandaloneReader streamWorker finished.", r.stat.Name)
}

// replayKeyViaCursor reads one key incrementally with a type-native cursor
// (ZSCAN/HSCAN/SSCAN/LRANGE/GET) on the given connection and emits it as batched write
// commands — NEVER DUMP. TYPE is re-checked here (not trusted from classification) so a
// key whose type changed, or vanished, between SCAN and now is still handled correctly.
func (r *scanStandaloneReader) replayKeyViaCursor(c *client.Redis, dbId int, key string) {
	typ := c.DoWithStringReply("TYPE", key)
	switch typ {
	case "none":
		return // key vanished between SCAN and now
	case "zset":
		r.streamCursorPairs(c, dbId, key, "ZSCAN", "ZADD", true) // ZSCAN(member,score) -> ZADD score member
	case "hash":
		r.streamCursorPairs(c, dbId, key, "HSCAN", "HSET", false) // HSCAN(field,value) -> HSET field value
	case "set":
		r.streamCursorMembers(c, dbId, key, "SSCAN", "SADD")
	case "list":
		r.streamList(c, dbId, key)
	case "string":
		if v := c.Do("GET", key); v != nil {
			r.emitBig(dbId, []string{"SET", key, v.(string)})
		}
	default:
		log.Panicf("[%s] replayKeyViaCursor unsupported type=[%s] key=[%s]", r.stat.Name, typ, key)
	}

	if p, ok := c.Do("PTTL", key).(int64); ok && p > 0 {
		r.emitBig(dbId, []string{"PEXPIRE", key, strconv.FormatInt(p, 10)})
	}
}

// streamCursorPairs handles ZSCAN/HSCAN: each cursor page is a flat [a,b,a,b,...]
// list. swap=true emits (b,a) per pair (ZSCAN yields member,score but ZADD wants
// score,member); swap=false keeps order (HSCAN field,value -> HSET field value).
func (r *scanStandaloneReader) streamCursorPairs(c *client.Redis, dbId int, key, scanCmd, writeCmd string, swap bool) {
	const scanCount = 4096
	const batchPairs = 256
	var cursor uint64 = 0
	for {
		arr := c.Do(scanCmd, key, strconv.FormatUint(cursor, 10), "COUNT", scanCount).([]interface{})
		cursor, _ = strconv.ParseUint(arr[0].(string), 10, 64)
		items := arr[1].([]interface{})
		argv := []string{writeCmd, key}
		for i := 0; i+1 < len(items); i += 2 {
			a, b := items[i].(string), items[i+1].(string)
			if swap {
				argv = append(argv, b, a)
			} else {
				argv = append(argv, a, b)
			}
			if len(argv) >= 2+2*batchPairs {
				r.emitBig(dbId, argv)
				argv = []string{writeCmd, key}
			}
		}
		if len(argv) > 2 {
			r.emitBig(dbId, argv)
		}
		if cursor == 0 {
			return
		}
	}
}

// streamCursorMembers handles SSCAN: each page is a flat list of members.
func (r *scanStandaloneReader) streamCursorMembers(c *client.Redis, dbId int, key, scanCmd, writeCmd string) {
	const scanCount = 4096
	const batchN = 512
	var cursor uint64 = 0
	for {
		arr := c.Do(scanCmd, key, strconv.FormatUint(cursor, 10), "COUNT", scanCount).([]interface{})
		cursor, _ = strconv.ParseUint(arr[0].(string), 10, 64)
		items := arr[1].([]interface{})
		argv := []string{writeCmd, key}
		for _, it := range items {
			argv = append(argv, it.(string))
			if len(argv) >= 2+batchN {
				r.emitBig(dbId, argv)
				argv = []string{writeCmd, key}
			}
		}
		if len(argv) > 2 {
			r.emitBig(dbId, argv)
		}
		if cursor == 0 {
			return
		}
	}
}

// streamList replays a list in LRANGE chunks as RPUSH batches (lists have no cursor).
func (r *scanStandaloneReader) streamList(c *client.Redis, dbId int, key string) {
	const chunk = 512
	var start int64 = 0
	for {
		reply, ok := c.Do("LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(start+chunk-1, 10)).([]interface{})
		if !ok || len(reply) == 0 {
			return
		}
		argv := []string{"RPUSH", key}
		for _, it := range reply {
			argv = append(argv, it.(string))
		}
		r.emitBig(dbId, argv)
		if len(reply) < chunk {
			return
		}
		start += chunk
	}
}

func (r *scanStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *scanStandaloneReader) StatusString() string {
	if r.stat.ScanFinished {
		return fmt.Sprintf("need_update_count=[%d]", r.stat.NeedUpdateCount)
	}
	return fmt.Sprintf("scan_dbid=[%d], scan_percent=[%s], need_update_count=[%d]", r.stat.ScanDbId, r.stat.ScanPercentByDbId, r.stat.NeedUpdateCount)
}

func (r *scanStandaloneReader) StatusConsistent() bool {
	return r.stat.ScanFinished && r.stat.NeedUpdateCount == 0
}
