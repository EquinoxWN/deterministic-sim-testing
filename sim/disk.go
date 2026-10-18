package sim

import (
	"maps"
	"slices"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

type file struct {
	data   []byte
	synced int
}

// Disk is an in-memory env.Disk that tracks how much of each file has been synced.
type Disk struct {
	files map[string]*file
}

// NewDisk returns an empty disk.
func NewDisk() *Disk { return &Disk{files: map[string]*file{}} }

// Append adds data to the named file.
func (d *Disk) Append(name string, data []byte) error {
	if !env.ValidName(name) {
		return env.ErrBadName
	}
	f := d.files[name]
	if f == nil {
		f = &file{}
		d.files[name] = f
	}
	if len(f.data)+len(data) > env.MaxFileSize {
		return env.ErrFileTooLarge
	}
	f.data = append(f.data, data...)
	return nil
}

// Sync marks everything appended so far as durable.
func (d *Disk) Sync(name string) error {
	if !env.ValidName(name) {
		return env.ErrBadName
	}
	f := d.files[name]
	if f == nil {
		return env.ErrNotFound
	}
	f.synced = len(f.data)
	return nil
}

// ReadFile returns a copy of the file, including bytes not yet synced.
func (d *Disk) ReadFile(name string) ([]byte, error) {
	if !env.ValidName(name) {
		return nil, env.ErrBadName
	}
	f := d.files[name]
	if f == nil {
		return nil, env.ErrNotFound
	}
	return slices.Clone(f.data), nil
}

// Synced returns how many bytes of the file are durable.
func (d *Disk) Synced(name string) int {
	if f := d.files[name]; f != nil {
		return f.synced
	}
	return 0
}

// Names lists the files in sorted order.
func (d *Disk) Names() []string { return slices.Sorted(maps.Keys(d.files)) }
