// Command raftkv-loadgen drives a cluster with concurrent clients and checks
// every result. It is the "client keeps writing and checking" half of the
// chaos test.
//
// Checks:
//   - own-key: each client appends unique tokens to its own key and reads it
//     back; the value must be exactly the acknowledged appends, in order, once
//     each. Any lost, duplicated or reordered write, or stale read, is a
//     WRONG READ and the program exits with status 1.
//   - -linearizability: operations on a few shared keys are recorded and the
//     history is checked with Porcupine at the end (best for runs of minutes,
//     not hours: checking cost grows with history length).
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Navneesh004/raftkv/kv"
	"github.com/Navneesh004/raftkv/kv/history"
	"github.com/Navneesh004/raftkv/node"
	"github.com/anishathalye/porcupine"
)

func main() {
	servers := flag.String("servers", "localhost:7000,localhost:7001,localhost:7002", "comma-separated server addresses")
	clients := flag.Int("clients", 8, "concurrent clients")
	duration := flag.Duration("duration", time.Minute, "how long to run")
	lin := flag.Bool("linearizability", false, "record shared-key history and check it with Porcupine at the end")
	viz := flag.String("viz", "history.html", "where to write Porcupine's visualization if the check fails")
	flag.Parse()

	addrs := strings.Split(*servers, ",")
	logger := log.New(os.Stdout, "[loadgen] ", log.Ltime)
	rec := history.NewRecorder()

	var ops, wrong atomic.Int64
	var latMu sync.Mutex
	var lats []time.Duration
	var lastOK atomic.Int64 // unix nanos of the latest successful op
	var maxGap atomic.Int64
	lastOK.Store(time.Now().UnixNano())

	done := func(start time.Time) {
		now := time.Now()
		if gap := now.UnixNano() - lastOK.Swap(now.UnixNano()); gap > maxGap.Load() {
			maxGap.Store(gap)
		}
		ops.Add(1)
		latMu.Lock()
		lats = append(lats, now.Sub(start))
		latMu.Unlock()
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := kv.MakeClerk(node.Endpoints(addrs, 1500*time.Millisecond))
			own := fmt.Sprintf("own-%d-%d", c, time.Now().UnixNano())
			expected := ""
			for n := 0; !stop.Load(); n++ {
				t0 := time.Now()
				r := rand.IntN(100)
				switch {
				case n%200 == 0: // keep values short over long runs
					ck.Put(own, "")
					expected = ""
				case r < 50:
					tok := fmt.Sprintf("[%d]", n)
					ck.Append(own, tok)
					expected += tok
				case r < 80 || !*lin:
					if v := ck.Get(own); v != expected {
						wrong.Add(1)
						logger.Printf("WRONG READ client %d key %s:\n  got  %q\n  want %q", c, own, v, expected)
					}
				default:
					key := fmt.Sprintf("shared-%d", rand.IntN(3))
					var in history.Input
					var out history.Output
					call := rec.Now()
					switch rand.IntN(3) {
					case 0:
						in = history.Input{Op: "get", Key: key}
						out.Value = ck.Get(key)
					case 1:
						in = history.Input{Op: "put", Key: key, Value: fmt.Sprintf("p%d.%d ", c, n)}
						ck.Put(key, in.Value)
					default:
						in = history.Input{Op: "append", Key: key, Value: fmt.Sprintf("a%d.%d ", c, n)}
						ck.Append(key, in.Value)
					}
					rec.Add(c, in, out, call, rec.Now())
				}
				done(t0)
			}
		}(c)
	}

	start := time.Now()
	tick := time.NewTicker(time.Second)
	var prev int64
	for time.Since(start) < *duration {
		<-tick.C
		latMu.Lock()
		p50, p99 := percentiles(lats)
		lats = lats[:0]
		latMu.Unlock()
		cur := ops.Load()
		logger.Printf("t=%4.0fs  ops/s=%5d  p50=%-8v p99=%-8v  wrong=%d", time.Since(start).Seconds(), cur-prev, p50, p99, wrong.Load())
		prev = cur
	}
	stop.Store(true)
	wg.Wait()

	logger.Printf("done: %d ops in %v, longest gap with no successful op: %v, wrong reads: %d",
		ops.Load(), time.Since(start).Round(time.Second), time.Duration(maxGap.Load()).Round(time.Millisecond), wrong.Load())

	exit := 0
	if wrong.Load() > 0 {
		exit = 1
	}
	if *lin {
		logger.Printf("checking %d shared-key operations for linearizability...", rec.Len())
		res, info := rec.Check(2 * time.Minute)
		logger.Printf("linearizability: %s", res)
		if res == porcupine.Illegal {
			if err := history.Visualize(info, *viz); err == nil {
				logger.Printf("visualization written to %s", *viz)
			}
			exit = 1
		}
	}
	os.Exit(exit)
}

func percentiles(d []time.Duration) (p50, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2].Round(100 * time.Microsecond), s[len(s)*99/100].Round(100 * time.Microsecond)
}
