// Package kv is a small primary-backup replicated key-value store, the example service the
// framework tests. It reaches time, the network, the disk and randomness only through env.Env,
// so the same code runs in the simulator and on the real runtime.
//
// Protocol: node 1 is the primary. It orders every write, appends it to its write-ahead log,
// sends it to every backup and answers the client once every backup has acknowledged it. Backups
// apply writes in sequence order and acknowledge the highest contiguous sequence they hold. The
// primary resends unacknowledged writes on a timer, and clients retry on a timeout, so lost and
// duplicated messages are tolerated.
package kv

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

// PrimaryID is the node that orders writes.
const PrimaryID env.NodeID = 1

const (
	walName   = "wal"
	maxKey    = 128
	maxValue  = 1024
	maxBuffer = 4096 // out-of-order writes a backup holds
	window    = 64   // writes resent per backup per tick
)

const (
	tPut   = "put"
	tGet   = "get"
	tReply = "reply"
	tRepl  = "repl"
	tAck   = "ack"
)

type message struct {
	Type  string `json:"t"`
	Req   uint64 `json:"r,omitempty"`
	Seq   uint64 `json:"s,omitempty"`
	Key   string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
	Found bool   `json:"f,omitempty"`
}

type entry struct {
	Seq   uint64 `json:"s"`
	Key   string `json:"k"`
	Value string `json:"v"`
}

// encode serialises a message.
func encode(m message) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err) // a struct of strings and integers always marshals
	}
	return b
}

// decode parses a message strictly; malformed input is rejected.
func decode(p []byte) (message, bool) {
	var m message
	if len(p) > env.MaxMessage {
		return m, false
	}
	d := json.NewDecoder(bytes.NewReader(p))
	d.DisallowUnknownFields()
	if d.Decode(&m) != nil || len(m.Key) > maxKey || len(m.Value) > maxValue {
		return m, false
	}
	return m, true
}

// ReplicaConfig configures a primary or a backup.
type ReplicaConfig struct {
	Backups         []env.NodeID  // every backup, known to all replicas
	RetransmitEvery time.Duration // primary resend period
	// ArrivalOrderBug makes a backup apply writes in arrival order and acknowledge the highest
	// sequence it has seen: a deliberate bug the simulator is expected to find.
	ArrivalOrderBug bool
}

type waiter struct {
	client env.NodeID
	req    uint64
}

type session struct {
	req   uint64
	done  bool
	reply message
}

// Replica is a primary or a backup, depending on its node ID.
type Replica struct {
	cfg   ReplicaConfig
	e     env.Env
	state map[string]string

	log        []entry
	acked      map[env.NodeID]uint64
	committed  uint64
	waiting    map[uint64]waiter
	sessions   map[env.NodeID]*session
	retransmit env.Timer

	applied uint64
	buffer  map[uint64]entry
	seen    map[uint64]bool
}

// NewReplica returns a replica; its role follows from the ID it is started with.
func NewReplica(cfg ReplicaConfig) *Replica {
	return &Replica{cfg: cfg}
}

// Start initialises the replica.
func (r *Replica) Start(e env.Env) {
	r.e = e
	r.state = map[string]string{}
	r.acked = map[env.NodeID]uint64{}
	r.waiting = map[uint64]waiter{}
	r.sessions = map[env.NodeID]*session{}
	r.buffer = map[uint64]entry{}
	r.seen = map[uint64]bool{}
}

// Receive dispatches one message by role and type.
func (r *Replica) Receive(from env.NodeID, p []byte) {
	m, ok := decode(p)
	if !ok {
		return
	}
	if r.e.ID() != PrimaryID {
		if from == PrimaryID && m.Type == tRepl && m.Seq > 0 {
			r.onReplicate(entry{Seq: m.Seq, Key: m.Key, Value: m.Value})
		}
		return
	}
	switch {
	case m.Type == tAck && slices.Contains(r.cfg.Backups, from):
		r.onAck(from, m.Seq)
	case m.Type == tPut && r.isClient(from):
		r.onPut(from, m)
	case m.Type == tGet && r.isClient(from):
		r.onGet(from, m)
	}
}

// isClient reports whether a node is neither the primary nor a backup.
func (r *Replica) isClient(id env.NodeID) bool {
	return id != PrimaryID && !slices.Contains(r.cfg.Backups, id)
}

// session returns the client's deduplication state.
func (r *Replica) session(c env.NodeID) *session {
	s := r.sessions[c]
	if s == nil {
		s = &session{}
		r.sessions[c] = s
	}
	return s
}

// onPut orders a new write or answers a retry.
func (r *Replica) onPut(c env.NodeID, m message) {
	s := r.session(c)
	if m.Req < s.req {
		return
	}
	if m.Req == s.req {
		if s.done {
			r.send(c, s.reply)
		}
		return
	}
	s.req, s.done = m.Req, false
	e := entry{Seq: uint64(len(r.log)) + 1, Key: m.Key, Value: m.Value}
	r.log = append(r.log, e)
	r.walAppend(e)
	r.walSync()
	r.waiting[e.Seq] = waiter{client: c, req: m.Req}
	for _, b := range r.cfg.Backups {
		r.send(b, message{Type: tRepl, Seq: e.Seq, Key: e.Key, Value: e.Value})
	}
	r.advance()
	r.arm()
}

// onGet answers a read from committed state.
func (r *Replica) onGet(c env.NodeID, m message) {
	s := r.session(c)
	if m.Req < s.req || (m.Req == s.req && !s.done) {
		return
	}
	if m.Req > s.req {
		v, found := r.state[m.Key]
		*s = session{req: m.Req, done: true, reply: message{Type: tReply, Req: m.Req, Key: m.Key, Value: v, Found: found}}
	}
	r.send(c, s.reply)
}

// onAck records a backup's cumulative acknowledgement.
func (r *Replica) onAck(b env.NodeID, seq uint64) {
	if seq > r.acked[b] && seq <= uint64(len(r.log)) {
		r.acked[b] = seq
		r.advance()
	}
}

// advance commits every write all backups hold and answers its client.
func (r *Replica) advance() {
	low := uint64(len(r.log))
	for _, b := range r.cfg.Backups {
		low = min(low, r.acked[b])
	}
	for r.committed < low {
		r.committed++
		e := r.log[r.committed-1]
		r.state[e.Key] = e.Value
		w, ok := r.waiting[e.Seq]
		if !ok {
			continue
		}
		delete(r.waiting, e.Seq)
		if s := r.sessions[w.client]; s.req == w.req {
			s.done = true
			s.reply = message{Type: tReply, Req: w.req, Key: e.Key, Value: e.Value}
			r.send(w.client, s.reply)
		}
	}
	if r.committed == uint64(len(r.log)) && r.retransmit != nil {
		r.retransmit.Stop()
		r.retransmit = nil
	}
}

// arm starts the resend timer while writes are uncommitted.
func (r *Replica) arm() {
	if r.retransmit == nil && r.committed < uint64(len(r.log)) {
		r.retransmit = r.e.AfterFunc(r.cfg.RetransmitEvery, r.onRetransmit)
	}
}

// onRetransmit resends the writes each backup has not acknowledged.
func (r *Replica) onRetransmit() {
	r.retransmit = nil
	for _, b := range r.cfg.Backups {
		for s := r.acked[b] + 1; s <= uint64(len(r.log)) && s <= r.acked[b]+window; s++ {
			e := r.log[s-1]
			r.send(b, message{Type: tRepl, Seq: e.Seq, Key: e.Key, Value: e.Value})
		}
	}
	r.arm()
}

// onReplicate applies writes on a backup and acknowledges.
func (r *Replica) onReplicate(e entry) {
	if r.cfg.ArrivalOrderBug {
		if !r.seen[e.Seq] {
			r.seen[e.Seq] = true
			r.state[e.Key] = e.Value
			r.walAppend(e)
			r.walSync()
			r.applied = max(r.applied, e.Seq)
		}
		r.send(PrimaryID, message{Type: tAck, Seq: r.applied})
		return
	}
	if _, held := r.buffer[e.Seq]; e.Seq > r.applied && !held && e.Seq <= r.applied+maxBuffer {
		r.buffer[e.Seq] = e
	}
	progressed := false
	for {
		next, ok := r.buffer[r.applied+1]
		if !ok {
			break
		}
		delete(r.buffer, next.Seq)
		r.applied++
		r.state[next.Key] = next.Value
		r.walAppend(next)
		progressed = true
	}
	if progressed {
		r.walSync()
	}
	r.send(PrimaryID, message{Type: tAck, Seq: r.applied})
}

// send ignores delivery errors: the protocol retries.
func (r *Replica) send(to env.NodeID, m message) { _ = r.e.Send(to, encode(m)) }

// walAppend writes one log line; a disk error stops the node.
func (r *Replica) walAppend(e entry) {
	b, err := json.Marshal(e)
	if err == nil {
		err = r.e.Append(walName, append(b, '\n'))
	}
	if err != nil {
		panic(fmt.Sprintf("kv: node %d: wal append: %v", r.e.ID(), err))
	}
}

// walSync makes the log durable; a disk error stops the node.
func (r *Replica) walSync() {
	if err := r.e.Sync(walName); err != nil {
		panic(fmt.Sprintf("kv: node %d: wal sync: %v", r.e.ID(), err))
	}
}

// State returns a copy of the applied (on the primary: committed) state.
func (r *Replica) State() map[string]string { return maps.Clone(r.state) }

// StateHash fingerprints the state.
func (r *Replica) StateHash() string { return HashState(r.state) }

// Progress returns the log length, the committed sequence and the applied sequence.
func (r *Replica) Progress() (logLen, committed, applied uint64) {
	return uint64(len(r.log)), r.committed, r.applied
}

// HashState fingerprints a state map independently of iteration order.
func HashState(state map[string]string) string {
	h := sha256.New()
	for _, k := range slices.Sorted(maps.Keys(state)) {
		fmt.Fprintf(h, "%q=%q\n", k, state[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ReplayWAL rebuilds a state from a write-ahead log in file order.
func ReplayWAL(data []byte) (map[string]string, error) {
	state := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 4096), env.MaxMessage)
	for line := 1; sc.Scan(); line++ {
		var e entry
		d := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		d.DisallowUnknownFields()
		if err := d.Decode(&e); err != nil {
			return nil, fmt.Errorf("kv: wal line %d: %w", line, err)
		}
		state[e.Key] = e.Value
	}
	return state, sc.Err()
}

// ClientConfig configures a client's workload.
type ClientConfig struct {
	Ops     int           // operations to complete
	Keys    int           // keys this client owns
	Timeout time.Duration // retry after this long, plus up to half of it as jitter
}

// Client issues one operation at a time on its own keys and checks every read against the
// writes it has seen acknowledged.
type Client struct {
	cfg        ClientConfig
	e          env.Env
	req        uint64
	op         message
	model      map[string]string
	done       int
	retries    int
	violations []string
	timer      env.Timer
}

// NewClient returns a client.
func NewClient(cfg ClientConfig) *Client { return &Client{cfg: cfg} }

// Start begins the workload.
func (c *Client) Start(e env.Env) {
	c.e = e
	c.model = map[string]string{}
	c.next()
}

// next issues the next random operation.
func (c *Client) next() {
	if c.done >= c.cfg.Ops {
		return
	}
	c.req++
	key := fmt.Sprintf("c%d-k%d", c.e.ID(), c.e.IntN(c.cfg.Keys))
	if c.e.IntN(2) == 0 {
		c.op = message{Type: tPut, Req: c.req, Key: key, Value: fmt.Sprintf("v%d-%d", c.e.ID(), c.req)}
	} else {
		c.op = message{Type: tGet, Req: c.req, Key: key}
	}
	c.sendOp()
}

// sendOp sends the current operation and arms the retry timer.
func (c *Client) sendOp() {
	_ = c.e.Send(PrimaryID, encode(c.op))
	jitter := time.Duration(c.e.IntN(int(c.cfg.Timeout/2) + 1))
	c.timer = c.e.AfterFunc(c.cfg.Timeout+jitter, c.onTimeout)
}

// onTimeout retries the same request.
func (c *Client) onTimeout() {
	c.retries++
	c.sendOp()
}

// Receive completes the current operation when its reply arrives.
func (c *Client) Receive(from env.NodeID, p []byte) {
	m, ok := decode(p)
	if !ok || from != PrimaryID || m.Type != tReply || c.op.Type == "" || m.Req != c.req {
		return
	}
	c.timer.Stop()
	if c.op.Type == tPut {
		c.model[c.op.Key] = c.op.Value
	} else {
		want, wantFound := c.model[c.op.Key]
		if m.Found != wantFound || m.Value != want {
			c.violations = append(c.violations, fmt.Sprintf(
				"client %d req %d: read %s = %q (found %v), last acknowledged write %q (found %v)",
				c.e.ID(), c.req, c.op.Key, m.Value, m.Found, want, wantFound))
		}
	}
	c.done++
	c.op = message{}
	c.next()
}

// Completed returns how many operations finished.
func (c *Client) Completed() int { return c.done }

// Retries returns how many times a request was resent.
func (c *Client) Retries() int { return c.retries }

// Violations returns reads that did not match the last acknowledged write.
func (c *Client) Violations() []string { return slices.Clone(c.violations) }
