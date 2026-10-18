package env

import (
	"strings"
	"testing"
)

func TestValidName(t *testing.T) {
	good := []string{"wal", "wal.0001", "a_b-c.d", strings.Repeat("x", 64)}
	bad := []string{"", ".", "..", ".wal", "../etc", "a/b", `a\b`, "C:wal", "Wal", "a b", "wal\x00", strings.Repeat("x", 65), "é"}
	for _, n := range good {
		if !ValidName(n) {
			t.Errorf("ValidName(%q) = false", n)
		}
	}
	for _, n := range bad {
		if ValidName(n) {
			t.Errorf("ValidName(%q) = true", n)
		}
	}
}
