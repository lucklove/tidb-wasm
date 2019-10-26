package main

import (
	"context"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
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
	p := parser.New()

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
		var execSQL string
		if stmts, _, err := p.Parse(sql, "", ""); err != nil {
			return term.Error(err)
		} else {
			restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
				format.RestoreSpacesAroundBinaryOperation
			for i := range stmts {
				var sb strings.Builder
				switch v := stmts[i].(type) {
				case *ast.LoadDataStmt:
					path := args[2].Invoke()
					v.Path = path.String()
					restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
					if v.Restore(restoreCtx) != nil {
						return term.Error(err)
					}
					sb.WriteString(";")
					execSQL += sb.String()
				case *ast.LoadStatsStmt:
					path := args[2].Invoke()
					v.Path = path.String()
					restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
					if v.Restore(restoreCtx) != nil {
						return term.Error(err)
					}
					sb.WriteString(";")
					execSQL += sb.String()
				default:
					restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
					if v.Restore(restoreCtx) != nil {
						return term.Error(err)
					}
					sb.WriteString(";")
					execSQL += sb.String()
				}
			}
		}
		fmt.Println(execSQL)
		if rs, err := k.Exec(id, execSQL); err != nil {
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
