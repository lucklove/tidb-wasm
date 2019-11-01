package main

import (
	"context"
	"fmt"
	"strings"
	"syscall/js"
	"time"

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
	seid := k.CreateSession()

	js.Global().Set("executeSQL", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			start := time.Now()
			id := seid
			text := args[0].String()
			ret := ""
			for _, sql := range strings.Split(text, ";") {
				if strings.Trim(sql, " ") == "" {
					continue
				} else if strings.Trim(sql, " \n\t\r") == "source" {
					if err := k.ExecFile(id); err != nil {
						ret += term.Error(err)
					} else {
						ret += term.WriteEmpty(time.Now().Sub(start))
					}
					continue
				}
				fmt.Println(sql)
				if rs, err := k.Exec(id, sql); err != nil {
					ret += term.Error(err)
				} else if rs == nil {
					ret += term.WriteEmpty(time.Now().Sub(start))
				} else if rows, err := k.ResultSetToStringSlice(context.Background(), id, rs); err != nil {
					ret += term.Error(err)
				} else {
					msg := term.WriteRows(rs.Fields(), rows, time.Now().Sub(start))
					ret += msg
				}
			}
			fmt.Println(ret)
			args[1].Invoke(ret)
		}()
		return nil
	}))

	c := make(chan bool)
	<-c
}
