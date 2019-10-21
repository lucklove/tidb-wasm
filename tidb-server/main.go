package main

import (
	"context"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/session"
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
	session.DisableStats4Test()
	if _, err := session.BootstrapSession(store); err != nil {
		panic("bootstrap session failed")
	}

	return NewKit(store)
}


func main() {
	k := setup()
	term := NewTerm()

	for {
		sql := term.Read()
		if rs, err := k.Exec(sql); err != nil {
			term.Error(err)
		} else if rs == nil {
			continue
		} else if rows, err := session.ResultSetToStringSlice(context.Background(), k.se, rs); err != nil {
			term.Error(err)
		} else {
			term.Write(rs.Fields(), rows)
		}
	}
}