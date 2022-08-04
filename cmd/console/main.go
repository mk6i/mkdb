package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/mkaminski/bkdb/engine"
)

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
	for {
		fmt.Printf("\n%s> ", sess.CurDB)
		query, _ := reader.ReadString(';')
		if err := sess.ExecQuery(query); err != nil {
			fmt.Printf("error: %s\n", err.Error())
		}
	}
}
