package main

import (
	"context"
	"syscall/js"
	"time"
	"fmt"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

func setup() *Kit {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	if err != nil {
		panic("create mock tikv store failed")
	}
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	if _, err := session.BootstrapSession(store); err != nil {
		panic("bootstrap session failed")
	}
	return NewKit(store)
}

func main() {
	k := setup()
	term := NewTerm()

	js.Global().Set("createSession", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		fmt.Println("create session")
		return k.CreateSession()
	}))

	js.Global().Set("closeSession", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		id := args[0].Int()
		fmt.Println("close session")
		k.CloseSession(id)
		return nil
	}))

	js.Global().Set("executeSQL", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		start := time.Now()
		id := args[0].Int()
		sql := args[1].String()
		fmt.Println(sql)
		if rs, err := k.Exec(id, sql); err != nil {
			return term.Error(err)
		} else if rs == nil {
			return term.WriteEmpty(time.Now().Sub(start))
		} else if rows, err := k.ResultSetToStringSlice(context.Background(), id, rs); err != nil {
			return term.Error(err)
		} else {
			msg := term.WriteRows(rs.Fields(), rows, time.Now().Sub(start))
			fmt.Println(msg)
			return msg
		}
	}))

	c := make(chan bool)
	<-c
}
