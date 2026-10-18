package dstlint

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestExampleServiceIsClean(t *testing.T) {
	got, err := Check("../example/kv")
	if err != nil || len(got) != 0 {
		t.Fatalf("example/kv: %v %v", got, err)
	}
}

func TestReportsEveryForbiddenConstruct(t *testing.T) {
	got, err := Check("testdata/bad")
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, f := range got {
		lines = append(lines, f.Rule+"@"+strconv.Itoa(f.Pos.Line))
	}
	want := []string{"import@5", "import@6", "import@7", "import@8", "wall-clock@15", "goroutine@16", "select@17", "wall-clock@21"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("findings %v\nwant     %v", lines, want)
	}
	if s := got[4].String(); !strings.Contains(s, "bad.go:15:") || !strings.Contains(s, "time.Now reads the wall clock") {
		t.Fatalf("diagnostic format: %s", s)
	}
}

func TestDotImportOfTimeIsReported(t *testing.T) {
	got, err := Check("testdata/dot")
	if err != nil || len(got) != 1 || !strings.Contains(got[0].Msg, "dot import") {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestErrorsOnMissingOrEmptyPackage(t *testing.T) {
	if _, err := Check("testdata/empty"); err == nil {
		t.Error("empty directory accepted")
	}
	if _, err := Check("testdata/missing"); err == nil {
		t.Error("missing directory accepted")
	}
}
