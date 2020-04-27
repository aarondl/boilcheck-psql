package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFindTaggedCalls(t *testing.T) {
	t.Parallel()

	p, _ := filepath.Abs("testpackage")
	pkgs, err := loadPackages(p)
	if err != nil {
		t.Fatal(err)
	}

	if len(pkgs) != 1 {
		t.Error("should have gotten one package")
	}

	calls, warns := findTaggedCalls(pkgs)

	// helper function to examine calls succinctly
	checkCall := func(t *testing.T, i int, pkg string, line int, sql string, args ...string) {
		t.Helper()
		call := calls[i]
		if !strings.Contains(call.SQL, sql) {
			t.Errorf("call %d) sql should contain %q: %q", i, sql, call.SQL)
		}
		if call.Package != pkg {
			t.Errorf("call %d) package wrong: %s", i, call.Package)
		}
		if call.Pos.Line != line {
			t.Errorf("call %d) line wrong: %d", i, call.Pos.Line)
		}
		if !reflect.DeepEqual(call.ArgTypes, args) {
			t.Errorf("call %d) args wrong: %#v", i, call.ArgTypes)
		}
	}

	pkg := `github.com/volatiletech/sqlboiler/v4/cmd/boilcheck-psql/testpackage`
	two := `select * from users;` + string(os.PathSeparator)
	three := `select * from videos;`
	four := `select * from tags;`
	five := `select * from comments;`
	six := `select * from logins;`

	if want := 10; len(calls) != want {
		t.Error("there should be", want, "calls, got:", len(calls))
	}
	checkCall(t, 0, pkg, 49, two, "int")
	checkCall(t, 1, pkg, 55, two, "database/sql.NullBool")
	checkCall(t, 2, pkg, 61, three, "int")
	checkCall(t, 3, pkg, 68, `select * from logs;`, "database/sql.NullBool")
	checkCall(t, 4, pkg, 77, `select * from logs where id = $1;`, "int")
	checkCall(t, 5, pkg, 84, two, "int")
	checkCall(t, 6, pkg, 88, `select * from users;`, "int")
	checkCall(t, 7, pkg, 91, four, "int")
	checkCall(t, 8, pkg, 104, five, "int")
	checkCall(t, 9, pkg, 109, six, "int")

	if warns[0].Pos.Line != 11 {
		t.Error("warning had wrong line number:", warns[0].Pos.Line)
	}
	if !strings.Contains(warns[0].Err, "not a constant") {
		t.Error("warning was wrong:", warns[0].Err)
	}
	if warns[1].Pos.Line != 43 {
		t.Error("warning had wrong line number:", warns[1].Pos.Line)
	}
	if !strings.Contains(warns[1].Err, `argument "one" to sql function is not a constant`) {
		t.Error("warning was wrong:", warns[1].Err)
	}
}
