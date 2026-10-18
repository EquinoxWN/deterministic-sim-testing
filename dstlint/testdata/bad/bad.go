// Package bad holds one of every construct dstlint must report. It is never compiled.
package bad

import (
	"math/rand"
	"net/http"
	"os"
	"sync"
	clock "time"
)

var mu sync.Mutex

func work() {
	_ = clock.Now()
	go func() {}()
	select {}
	_ = rand.Int()
	_, _ = os.ReadFile("x")
	_ = http.StatusOK
	clock.Sleep(1)
	_ = clock.Duration(1) // a type, allowed
}
