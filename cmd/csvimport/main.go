package main

import (
	"fmt"

	"net/http"
	_ "net/http/pprof"

	"github.com/mkaminski/bkdb/engine"
)

func main() {

	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	sess := engine.Session{}
	err := sess.Import()
	if err != nil {
		fmt.Printf("failed to import: %s\n", err.Error())
	}
}
