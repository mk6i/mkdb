package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/mkaminski/bkdb/engine"
	"github.com/mkaminski/bkdb/storage"
)

func init() {
	if err := storage.InitStorage(); err != nil {
		panic(fmt.Sprintf("storage init error: %s", err.Error()))
	}
}

func main() {

	reader := bufio.NewReader(os.Stdin)

	fmt.Println(`
██████   ██████ █████   ████ ██████████   ███████████ 
░░██████ ██████ ░░███   ███░ ░░███░░░░███ ░░███░░░░░███
 ░███░█████░███  ░███  ███    ░███   ░░███ ░███    ░███
 ░███░░███ ░███  ░███████     ░███    ░███ ░██████████ 
 ░███ ░░░  ░███  ░███░░███    ░███    ░███ ░███░░░░░███
 ░███      ░███  ░███ ░░███   ░███    ███  ░███    ░███
 █████     █████ █████ ░░████ ██████████   ███████████ 
░░░░░     ░░░░░ ░░░░░   ░░░░ ░░░░░░░░░░   ░░░░░░░░░░░  
	`)

	sess := engine.Session{}
	defer sess.Close()

	for {
		fmt.Printf("\n%s> ", sess.CurDB)
		query, _ := reader.ReadString(';')
		if err := sess.ExecQuery(query); err != nil {
			fmt.Printf("error: %s\n", err.Error())
		}
	}
}
