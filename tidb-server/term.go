package main

import (
	"fmt"
	"os"
	"bufio"
	"strings"

	"github.com/pingcap/parser/ast"
)

type Terminal interface {
	Read() string
	Write([]*ast.ResultField, [][]string) error
	Error(error)
}

func NewTerm() Terminal {
	return &Term{bufio.NewReader(os.Stdin), []string{}}
}

type Term struct {
	reader *bufio.Reader
	commands []string
}

func (t *Term) Read() string {
	if len(t.commands) > 0 {
		cmd := t.commands[0]
		t.commands = t.commands[1:]
		return cmd
	}
	fmt.Print("TiDB [pingcap]> ")
	for {
		text, _ := t.reader.ReadString('\n')
		text = strings.Trim(text, " \n\t")
		cmds := strings.Split(text, ";")
		if t.appendCommands(cmds) {
			fmt.Print("...")
			continue
		} else {
			break
		}
	}
	if len(t.commands) > 0 {
		cmd := t.commands[0]
		t.commands = t.commands[1:]
		return cmd
	} else {
		return ""
	}
}

func (t *Term) appendCommands(cmds []string) (cont bool) {
	if len(cmds) == 0 {
		panic("append empty commands")
	}
	if cmds[len(cmds)-1] == "" {
		cmds = cmds[0:len(cmds)-1]
		if len(cmds) == 0 {
			return len(t.commands) != 0
		}
	} else {
		cont = true
	}
	if len(t.commands) == 0 {
		t.commands = cmds
	} else {
		l := len(t.commands)
		t.commands[l-1] = t.commands[l-1] + " " + cmds[0]
		t.commands = append(t.commands, cmds[1:]...)
	}
	return cont
}

type Column struct {
	Name string
	Values []string
	Len  int
}

func (t *Term) Write(fields []*ast.ResultField, rows [][]string) error {
	columns := make([]*Column, len(fields))
	for i := range columns {
		columns[i] = &Column{
			Name: fields[i].Column.Name.O,
			Len: len(fields[i].Column.Name.O),
		}
	}

	for i := range rows {
		for j, c := range columns {
			value := rows[i][j]
			c.Values = append(c.Values, value)
			if len(value) > c.Len {
				c.Len = len(value)
			}
		}
	}

	t.divider(columns)
	t.print(columns, -1)
	t.divider(columns)
	for idx := range rows {
		t.print(columns, idx)
		t.divider(columns)
	}

	return nil
}

func (*Term) divider(cs []*Column) {
	fmt.Print("+")
	for _, c := range cs {
		for i := 0; i < c.Len + 2; i++ {
			fmt.Print("-")
		}
		fmt.Print("+")
	}
	fmt.Println("")
}

func (*Term) print(cs []*Column, idx int) {
	fmt.Print("| ")
	for _, c := range cs {
		format := fmt.Sprintf("%%-%dv", c.Len)
		if idx < 0 {
			fmt.Printf(format, c.Name)
		} else {
			fmt.Printf(format, c.Values[idx])
		}
		fmt.Print(" | ")
	}
	fmt.Println("")
}

func (*Term) Error(err error) {
	fmt.Println(err)
}
