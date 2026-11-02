package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommands(t *testing.T) {
	dir := t.TempDir()
	trace := filepath.Join(dir, "trace.txt")
	cases := []struct {
		args []string
		code int
		want string
	}{
		{[]string{"run", "-seed", "1", "-trace", trace}, 0, "RESULT PASS"},
		{[]string{"run", "-seed", "2", "-bug"}, 1, "diverged from the primary"},
		{[]string{"sweep", "-count", "20"}, 0, "failed=0"},
		{[]string{"sweep", "-count", "50", "-bug"}, 1, "RESULT FAIL"},
		{[]string{"replay", "-seed", "2", "-bug", "-runs", "2"}, 0, "RESULT PASS: 2 runs of seed 2 are identical"},
		{[]string{"real", "-dir", filepath.Join(dir, "real")}, 0, "RESULT PASS"},
		{[]string{"lint", "../../example/kv"}, 0, "clean"},
		{[]string{"lint", "../../sim"}, 1, `"math/rand/v2" is not deterministic`},
	}
	for _, c := range cases {
		var out bytes.Buffer
		code, err := run(c.args, &out)
		if err != nil || code != c.code || !strings.Contains(out.String(), c.want) {
			t.Errorf("%v: code %d err %v output:\n%s", c.args, code, err, out.String())
		}
	}
	b, err := os.ReadFile(trace)
	if err != nil || !strings.HasPrefix(string(b), "0 start n") {
		t.Fatalf("trace file: %v %.40q", err, b)
	}
}

func TestUsageErrors(t *testing.T) {
	flagOutput = io.Discard
	t.Cleanup(func() { flagOutput = os.Stderr })
	for _, args := range [][]string{
		nil,
		{"nope"},
		{"run", "-seed", "x"},
		{"run", "extra"},
		{"sweep", "-count", "0"},
		{"sweep", "-count", "100001"},
		{"replay", "-runs", "1"},
		{"real", "-drop", "0.5"},
		{"lint"},
		{"lint", "no/such/dir"},
		{"run", "-clients", "0"},
	} {
		var out bytes.Buffer
		if code, err := run(args, &out); code != 2 || err == nil {
			t.Errorf("%v: code %d err %v", args, code, err)
		}
	}
}

func TestRealRefusesBadTimeoutBeforeEmptyingDir(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(keep, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"0s", "-1s", "11m"} {
		var out bytes.Buffer
		if code, err := run([]string{"real", "-dir", dir, "-timeout", d}, &out); code != 2 || err == nil {
			t.Errorf("-timeout %s: code %d err %v", d, code, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("directory emptied before the options were checked: %v", err)
	}
}
