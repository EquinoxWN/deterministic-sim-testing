// Package dstlint checks that a service package can only reach the world through env: it reports
// imports and calls that read ambient time, randomness, files, the network or the scheduler, any
// of which would make a simulated run depend on something other than the seed.
package dstlint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Finding is one forbidden construct.
type Finding struct {
	Pos  token.Position
	Rule string
	Msg  string
}

// String formats the finding like a compiler diagnostic.
func (f Finding) String() string { return fmt.Sprintf("%s: %s: %s", f.Pos, f.Rule, f.Msg) }

// forbiddenImports maps an import path to the env interface to use instead.
var forbiddenImports = map[string]string{
	"math/rand":    "env.Rand",
	"math/rand/v2": "env.Rand",
	"crypto/rand":  "env.Rand",
	"os":           "env.Disk",
	"os/exec":      "nothing: services do not start processes",
	"io/ioutil":    "env.Disk",
	"syscall":      "env",
	"net":          "env.Network",
	"sync":         "nothing: the runtime serialises callbacks",
	"sync/atomic":  "nothing: the runtime serialises callbacks",
	"runtime":      "nothing: scheduling belongs to the runtime",
	"unsafe":       "nothing",
}

// forbiddenTime lists time functions that read or wait on the wall clock.
var forbiddenTime = []string{"Now", "Since", "Until", "Sleep", "After", "AfterFunc", "NewTimer", "NewTicker", "Tick"}

// Check parses the non-test Go files in dir and returns every finding in file order.
func Check(dir string) ([]Finding, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var out []Finding
	files := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		files++
		out = append(out, checkFile(fset, f)...)
	}
	if files == 0 {
		return nil, fmt.Errorf("dstlint: no Go files in %s", dir)
	}
	return out, nil
}

// checkFile reports forbidden imports, wall-clock calls, goroutines and selects in one file.
func checkFile(fset *token.FileSet, f *ast.File) []Finding {
	var out []Finding
	add := func(p token.Pos, rule, msg string) {
		out = append(out, Finding{Pos: fset.Position(p), Rule: rule, Msg: msg})
	}
	timeName := ""
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if use, bad := forbiddenImports[path]; bad || strings.HasPrefix(path, "net/") {
			if !bad {
				use = "env.Network"
			}
			add(imp.Pos(), "import", fmt.Sprintf("%q is not deterministic; use %s", path, use))
		}
		if path == "time" {
			timeName = "time"
			if imp.Name != nil {
				timeName = imp.Name.Name
			}
			if timeName == "." {
				add(imp.Pos(), "import", `dot import of "time" hides wall-clock calls`)
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			add(n.Pos(), "goroutine", "go statements make scheduling nondeterministic; use env.Clock.AfterFunc")
		case *ast.SelectStmt:
			add(n.Pos(), "select", "select picks a ready case at random")
		case *ast.SelectorExpr:
			if id, ok := n.X.(*ast.Ident); ok && timeName != "" && id.Name == timeName && slices.Contains(forbiddenTime, n.Sel.Name) {
				add(n.Pos(), "wall-clock", fmt.Sprintf("time.%s reads the wall clock; use env.Clock", n.Sel.Name))
			}
		}
		return true
	})
	return out
}
