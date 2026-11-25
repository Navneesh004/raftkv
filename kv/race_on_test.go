//go:build race

package kv

// The race detector slows gob/reflection-heavy code by an order of magnitude,
// so timing assertions are relaxed when it is on.
const raceEnabled = true
