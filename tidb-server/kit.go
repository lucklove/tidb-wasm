package main

import (
	"context"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

type Kit struct {
	mu       sync.Mutex
	store    kv.Storage
	sessions map[int]session.Session
	nextID   int
}

func NewKit(store kv.Storage) *Kit {
	return &Kit{
		store:    store,
		sessions: make(map[int]session.Session),
	}
}

func (k *Kit) CreateSession() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	s, err := session.CreateSession(k.store)
	if err != nil {
		panic(err)
	}
	if !s.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "localhost"}, nil, nil) {
		panic("auth failed")
	}
	id := k.nextID
	k.nextID++
	k.sessions[id] = s
	return id
}

func (k *Kit) CloseSession(id int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	se, ok := k.sessions[id]
	if !ok {
		return
	}
	se.Close()
	delete(k.sessions, id)
}

func (k *Kit) Exec(id int, sql string) (sqlexec.RecordSet, error) {
	se, ok := k.sessions[id]
	if !ok {
		return nil, errors.New("session not exists")
	}

	ctx := context.Background()
	rss, err := se.Execute(ctx, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rss == nil {
		loadDataInfo := se.Value(executor.LoadDataVarKey)
		if loadDataInfo != nil {
			defer se.SetValue(executor.LoadDataVarKey, nil)
			if err = handleLoadData(ctx, se, loadDataInfo.(*executor.LoadDataInfo)); err != nil {
				return nil, err
			}
		}
	}

	if len(rss) > 0 {
		return rss[0], nil
	}

	loadStats := se.Value(executor.LoadStatsVarKey)
	if loadStats != nil {
		defer se.SetValue(executor.LoadStatsVarKey, nil)
		if err := k.handleLoadStats(ctx, loadStats.(*executor.LoadStatsInfo)); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return nil, errors.Trace(err)
}

func (k *Kit) ResultSetToStringSlice(ctx context.Context, id int, rs sqlexec.RecordSet) ([][]string, error) {
	se, ok := k.sessions[id]
	if !ok {
		return nil, errors.New("session not exists")
	}
	return session.ResultSetToStringSlice(context.Background(), se, rs)
}

// handleLoadStats does the additional work after processing the 'load stats' query.
// It sends client a file path, then reads the file content from client, loads it into the storage.
func (k *Kit) handleLoadStats(ctx context.Context, loadStatsInfo *executor.LoadStatsInfo) error {
	if loadStatsInfo == nil {
		return errors.New("load stats: info is empty")
	}

	c := make(chan error)
	js.Global().Get("upload").Invoke(js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		loadStatsInfo.Update([]byte(args[0].String()))
		c <- nil
		return nil
	}), js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		fmt.Println("on error")
		c <- errors.New(args[0].String())
		return nil
	}))

	select {
	case e := <-c:
		return e
	case <-time.After(10 * time.Second):
		return errors.New("upload timeout")
	}
	return <-c
}

func handleLoadData(ctx context.Context, se session.Session, loadDataInfo *executor.LoadDataInfo) error {
	if loadDataInfo == nil {
		return errors.New("load data info is empty")
	}
	loadDataInfo.InitQueues()
	loadDataInfo.SetMaxRowsInBatch(uint64(loadDataInfo.Ctx.GetSessionVars().DMLBatchSize))
	loadDataInfo.StartStopWatcher()

	err := loadDataInfo.Ctx.NewTxn(ctx)
	if err != nil {
		return err
	}

	go processData(ctx, loadDataInfo)

	err = loadDataInfo.CommitWork(ctx)
	loadDataInfo.SetMessage()

	var txn kv.Transaction
	var err1 error
	txn, err1 = loadDataInfo.Ctx.Txn(true)
	if err1 == nil {
		if txn != nil && txn.Valid() {
			if err != nil {
				txn.Rollback()
				return err
			}
			return se.CommitTxn(sessionctx.SetCommitCtx(ctx, loadDataInfo.Ctx))
		}
	}
	// Should never reach here.
	panic(err1)
}

func processData(ctx context.Context, loadDataInfo *executor.LoadDataInfo) {
	var err error
	var shouldBreak bool
	var prevData, curData []byte
	defer func() {
		r := recover()
		if err != nil || r != nil {
			loadDataInfo.ForceQuit()
		} else {
			loadDataInfo.CloseTaskQueue()
		}
	}()

	curData = []byte(js.Global().Call("loadData").String())
	for {
		if len(curData) == 0 {
			shouldBreak = true
			if len(prevData) == 0 {
				break
			}
		}
		select {
		case <-loadDataInfo.QuitCh:
			err = errors.New("processStream forced to quit")
		default:
		}
		if err != nil {
			break
		}
		// prepare batch and enqueue task
		prevData, err = insertDataWithCommit(ctx, prevData, curData, loadDataInfo)
		if err != nil {
			break
		}
		if shouldBreak {
			break
		}
	}
	if err == nil {
		loadDataInfo.EnqOneTask(ctx)
	}
}

func insertDataWithCommit(ctx context.Context, prevData,
	curData []byte, loadDataInfo *executor.LoadDataInfo) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = loadDataInfo.InsertData(ctx, prevData, curData)
		if err != nil {
			return nil, err
		}
		if !reachLimit {
			break
		}
		// push into commit task queue
		err = loadDataInfo.EnqOneTask(ctx)
		if err != nil {
			return prevData, err
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}
