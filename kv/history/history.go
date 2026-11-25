// Package history records the operations clients perform against the store
// and checks them for linearizability with Porcupine.
//
// A history is linearizable if every operation appears to take effect
// atomically at some instant between its call and its return, in an order
// consistent with what each operation returned. Porcupine searches for such
// an order; if none exists, the store returned something impossible.
package history

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Input describes an operation.
type Input struct {
	Op    string // "get", "put" or "append"
	Key   string
	Value string
}

// Output is what the operation returned (only meaningful for gets).
type Output struct {
	Value string
}

// Recorder collects operations from concurrent clients.
type Recorder struct {
	mu  sync.Mutex
	t0  time.Time
	ops []porcupine.Operation
}

// NewRecorder starts a new history.
func NewRecorder() *Recorder { return &Recorder{t0: time.Now()} }

// Now returns a monotonic timestamp for Call/Return.
func (r *Recorder) Now() int64 { return int64(time.Since(r.t0)) }

// Add records one completed operation.
func (r *Recorder) Add(client int, in Input, out Output, call, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
}

// Len is the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Check runs Porcupine on the history. Unknown means it ran out of time.
func (r *Recorder) Check(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()
	return porcupine.CheckOperationsVerbose(Model(), ops, timeout)
}

// Visualize writes Porcupine's interactive HTML view of a checked history.
func Visualize(info porcupine.LinearizationInfo, path string) error {
	return porcupine.VisualizePath(Model(), info, path)
}

// Model is the sequential specification of the store: a map from string to
// string. Keys are independent, so the history is partitioned per key, which
// makes checking dramatically faster.
func Model() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			for _, op := range history {
				k := op.Input.(Input).Key
				byKey[k] = append(byKey[k], op)
			}
			keys := make([]string, 0, len(byKey))
			for k := range byKey {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				out = append(out, byKey[k])
			}
			return out
		},
		Init: func() any { return "" }, // state of one key
		Step: func(state, input, output any) (bool, any) {
			st := state.(string)
			in := input.(Input)
			out := output.(Output)
			switch in.Op {
			case "get":
				return out.Value == st, st
			case "put":
				return true, in.Value
			case "append":
				return true, st + in.Value
			}
			return false, st
		},
		DescribeOperation: func(input, output any) string {
			in := input.(Input)
			switch in.Op {
			case "get":
				return fmt.Sprintf("get(%q) -> %q", in.Key, output.(Output).Value)
			case "put":
				return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
			default:
				return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
			}
		},
	}
}
