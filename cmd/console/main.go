package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	shutdownHandler(func() {
		sess.Close()
	})

	for {
		fmt.Printf("\n%s> ", sess.CurDB)
		query, _ := reader.ReadString(';')
		if err := sess.ExecQuery(query); err != nil {
			fmt.Printf("error: %s\n", err.Error())
		}
	}
}

func shutdownHandler(fn func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		for {
			s := <-ch
			switch s {
			case syscall.SIGHUP:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGTERM:
				fallthrough
			case syscall.SIGQUIT:
				fn()
				os.Exit(0)
			}
		}
	}()
}
