package sim

import (
	"errors"
	"reflect"
	"testing"

	"github.com/EquinoxWN/deterministic-sim-testing/env"
)

func TestDiskAppendSyncRead(t *testing.T) {
	d := NewDisk()
	if _, err := d.ReadFile("wal"); !errors.Is(err, env.ErrNotFound) {
		t.Fatalf("missing file: %v", err)
	}
	if err := d.Sync("wal"); !errors.Is(err, env.ErrNotFound) {
		t.Fatalf("sync missing: %v", err)
	}
	_ = d.Append("wal", []byte("ab"))
	_ = d.Sync("wal")
	_ = d.Append("wal", []byte("cd"))
	_ = d.Append("meta", nil)
	got, _ := d.ReadFile("wal")
	if string(got) != "abcd" || d.Synced("wal") != 2 || d.Synced("none") != 0 {
		t.Fatalf("read %q synced %d", got, d.Synced("wal"))
	}
	got[0] = 'X'
	again, _ := d.ReadFile("wal")
	if string(again) != "abcd" {
		t.Fatal("ReadFile returned shared memory")
	}
	if !reflect.DeepEqual(d.Names(), []string{"meta", "wal"}) {
		t.Fatalf("names %v", d.Names())
	}
}

func TestDiskRejectsBadNamesAndHugeFiles(t *testing.T) {
	d := NewDisk()
	for _, name := range []string{"", "../x", "a/b", `a\b`, ".hidden", "UPPER", string(make([]byte, 65))} {
		if err := d.Append(name, nil); !errors.Is(err, env.ErrBadName) {
			t.Errorf("Append(%q) = %v", name, err)
		}
		if _, err := d.ReadFile(name); !errors.Is(err, env.ErrBadName) {
			t.Errorf("ReadFile(%q) = %v", name, err)
		}
		if err := d.Sync(name); !errors.Is(err, env.ErrBadName) {
			t.Errorf("Sync(%q) = %v", name, err)
		}
	}
	if err := d.Append("big", make([]byte, env.MaxFileSize+1)); !errors.Is(err, env.ErrFileTooLarge) {
		t.Fatalf("huge append: %v", err)
	}
}
