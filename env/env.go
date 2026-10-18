// Package env defines the only ways a service may observe time, the network, the disk and
// randomness. The sim package implements these interfaces with a seeded, single-threaded
// simulation; the realenv package implements them with the wall clock, UDP and files.
//
// Every callback a runtime makes into a node (Start, Receive and timer functions) runs one at a
// time for that node, so services need no locks.
package env

import (
	"errors"
	"time"
)

// NodeID names one node of a cluster.
type NodeID uint32

// MaxMessage is the largest payload Send accepts, in bytes.
const MaxMessage = 64 << 10

// MaxFileSize is the largest file a Disk keeps, in bytes.
const MaxFileSize = 64 << 20

// Errors returned by every implementation.
var (
	ErrMessageTooLarge = errors.New("env: message larger than MaxMessage")
	ErrUnknownNode     = errors.New("env: unknown destination node")
	ErrBadName         = errors.New("env: invalid file name")
	ErrNotFound        = errors.New("env: file not found")
	ErrFileTooLarge    = errors.New("env: file larger than MaxFileSize")
)

// Timer is a pending callback that can be cancelled.
type Timer interface {
	// Stop cancels the callback; it reports whether the callback had not run yet.
	Stop() bool
}

// Clock tells the time and schedules callbacks.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// AfterFunc runs f on the node after d.
	AfterFunc(d time.Duration, f func()) Timer
}

// Network sends messages to other nodes; delivery is not guaranteed.
type Network interface {
	// Send queues payload for node to; the payload is copied.
	Send(to NodeID, payload []byte) error
}

// Disk is a node's private append-only file store.
type Disk interface {
	// Append adds data to the end of the named file, creating it if needed.
	Append(name string, data []byte) error
	// Sync makes everything appended so far durable.
	Sync(name string) error
	// ReadFile returns the file's contents.
	ReadFile(name string) ([]byte, error)
}

// Rand is the node's source of randomness.
type Rand interface {
	// Uint64 returns a random 64-bit value.
	Uint64() uint64
	// IntN returns a value in [0, n); it panics if n <= 0.
	IntN(n int) int
	// Float64 returns a value in [0, 1).
	Float64() float64
}

// Env is everything a node may use to interact with the world.
type Env interface {
	// ID returns this node's identity.
	ID() NodeID
	Clock
	Network
	Disk
	Rand
}

// Node is a service instance driven by a runtime.
type Node interface {
	// Start is called once before any message is delivered.
	Start(e Env)
	// Receive handles one message from another node.
	Receive(from NodeID, payload []byte)
}

// ValidName reports whether a file name is safe on every Disk implementation.
func ValidName(name string) bool {
	if name == "" || len(name) > 64 || name[0] == '.' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}
