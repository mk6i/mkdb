package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mkaminski/bkdb/engine"
	"golang.org/x/term"
)

func init() {
	// if err := storage.InitStorage(); err != nil {
	// 	panic(fmt.Sprintf("storage init error: %s", err.Error()))
	// }
}

func main() {
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

	sess := &engine.Session{}

	shutdownHandler(func() {
		sess.Close()
	})

	if err := runTerminal(sess); err != nil {
		fmt.Printf("error: %s\n\r", err.Error())
	}
}

func runTerminal(sess *engine.Session) error {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		fmt.Println(fmt.Errorf("stdin/stdout should be terminal"))
	}

	t := NewTerminal(struct {
		io.Reader
		io.Writer
	}{os.Stdin, os.Stdout}, "")

	t.SetPrompt(fmt.Sprintf("%s > %s", t.Escape.Green, t.Escape.Reset))

	oldTerm, err := term.MakeRaw(0)
	if err != nil {
		return err
	}
	defer term.Restore(0, oldTerm)

	for {
		lines, err := t.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for _, query := range lines {
			if err := sess.ExecQuery(query); err != nil {
				fmt.Printf("error: %s\n\r", err.Error())
			}
		}
	}

	return nil
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
