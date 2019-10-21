package main

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/sqlexec"
)

type Kit struct {
	store kv.Storage
	se    session.Session
}

func NewKit(store kv.Storage) *Kit {
	se, err := session.CreateSession(store)
	if err != nil {
		panic(err)
	}

	return &Kit{
		store: store,
		se: se,
	}
}

func (k *Kit) Exec(sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		rss, err := k.se.Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, errors.Trace(err)
	}
	stmtID, _, _, err := k.se.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := make([]types.Datum, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewDatum(args[i])
	}
	rs, err := k.se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = k.se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rs, nil
}