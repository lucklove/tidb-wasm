package main

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/types"
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
	se, ok := k.sessions[id]
	if !ok {
		return
	}
	se.Close()
}

func (k *Kit) Exec(id int, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	se, ok := k.sessions[id]
	if !ok {
		return nil, errors.New("session not exists")
	}
	ctx := context.Background()
	if len(args) == 0 {
		rss, err := se.Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, errors.Trace(err)
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}
	rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rs, nil
}

func (k *Kit) ResultSetToStringSlice(ctx context.Context, id int, rs sqlexec.RecordSet) ([][]string, error) {
	se, ok := k.sessions[id]
	if !ok {
		return nil, errors.New("session not exists")
	}
	return session.ResultSetToStringSlice(context.Background(), se, rs)
}
