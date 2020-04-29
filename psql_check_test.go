package main

import (
	"flag"
	"go/token"
	"os"
	"testing"

	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/importers"
)

func TestMain(m *testing.M) {
	flag.Parse()
	flagDebug = testing.Verbose()
	code := m.Run()
	os.Exit(code)
}

func TestUnknownIdentifiers(t *testing.T) {
	t.Parallel()

	t.Run("Select", func(t *testing.T) {
		t.Parallel()

		t.Run("Minimal", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs, IdentErr{
				Table:    "users",
				Location: 14,
			})
		})
		t.Run("Ambiguous", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select id from users, videos;`)
			errs := checkCalls(&State{DBInfo: &drivers.DBInfo{
				Tables: []drivers.Table{
					{Name: "users", Columns: []drivers.Column{{Name: "id"}}},
					{Name: "videos", Columns: []drivers.Column{{Name: "id"}}},
				}},
			}, []Call{call})
			checkErrs(t, errs, IdentErr{
				Kind:     Ambiguous,
				Column:   "id",
				Location: 7,
			})
		})
		t.Run("MinimalSchema", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from public.users;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs, IdentErr{
				Schema:   "public",
				Table:    "users",
				Location: 14,
			})
		})
		t.Run("Quoted", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from "users";`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs, IdentErr{
				Table:    "users",
				Location: 14,
			})
		})
		t.Run("Column", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select id from users;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 15},
				IdentErr{Column: "id", Location: 7},
			)
		})
		t.Run("Column2", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select users.id from users;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 21},
				IdentErr{Table: "users", Column: "id", Location: 7},
			)
		})
		t.Run("Column3", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select public.users.id from users;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 28},
				IdentErr{Schema: "public", Table: "users", Column: "id", Location: 7},
			)
		})
		t.Run("Column3Quoted", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select "public"."users"."id" from "users";`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 34},
				IdentErr{Schema: "public", Table: "users", Column: "id", Location: 7},
			)
		})
		t.Run("Alias", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select u.id from users as u;`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 17},
				IdentErr{Table: "u", Column: "id", Location: 7},
			)
		})
		t.Run("Order", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users order by id`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Column: "id", Location: 29},
			)
		})
		t.Run("OrderSpecific", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users as u order by u.id`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Table: "u", Column: "id", Location: 34},
			)
		})
		t.Run("GroupHaving", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users as u group by u.id having u.id > 5`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Table: "u", Column: "id", Location: 46},
				IdentErr{Table: "u", Column: "id", Location: 34},
			)
		})
		t.Run("WhereEqual", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users where users.id = 5`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Table: "users", Column: "id", Location: 26},
			)
		})
		t.Run("WhereFunction", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users where length(users.id) = 5`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Table: "users", Column: "id", Location: 33},
			)
		})
		t.Run("WhereBools", func(t *testing.T) {
			t.Parallel()

			call := testCall(`select * from users where ((users.id = 5) and (users.name = true));`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 14},
				IdentErr{Table: "users", Column: "id", Location: 28},
				IdentErr{Table: "users", Column: "name", Location: 47},
			)
		})
		t.Run("InnerJoin", func(t *testing.T) {
			t.Parallel()

			call := testCall(`
			select *
			from users
			inner join videos on videos.user_id = users.id
			inner join comments on comments.video_id = videos.id
			`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 21},
				IdentErr{Table: "videos", Location: 41},
				IdentErr{Table: "videos", Column: "user_id", Location: 51},
				IdentErr{Table: "users", Column: "id", Location: 68},
				IdentErr{Table: "comments", Location: 91},
				IdentErr{Table: "comments", Column: "video_id", Location: 103},
				IdentErr{Table: "videos", Column: "id", Location: 123},
			)
		})
		t.Run("InnerJoinAlias", func(t *testing.T) {
			t.Parallel()

			call := testCall(`
			select *
			from users
			inner join videos vid on vid.user_id = users.id
			inner join comments on comments.video_id = vid.id
			`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 21},
				IdentErr{Table: "videos", Location: 41},
				IdentErr{Table: "vid", Column: "user_id", Location: 55},
				IdentErr{Table: "users", Column: "id", Location: 69},
				IdentErr{Table: "comments", Location: 92},
				IdentErr{Table: "comments", Column: "video_id", Location: 104},
				IdentErr{Table: "vid", Column: "id", Location: 124},
			)
		})
		t.Run("LeftJoin", func(t *testing.T) {
			t.Parallel()

			call := testCall(`
			select *
			from users
			left join videos on videos.user_id = users.id
			left join comments on comments.video_id = videos.id
			`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 21},
				IdentErr{Table: "videos", Location: 40},
				IdentErr{Table: "videos", Column: "user_id", Location: 50},
				IdentErr{Table: "users", Column: "id", Location: 67},
				IdentErr{Table: "comments", Location: 89},
				IdentErr{Table: "comments", Column: "video_id", Location: 101},
				IdentErr{Table: "videos", Column: "id", Location: 121},
			)
		})
		t.Run("LateralJoin", func(t *testing.T) {
			t.Parallel()

			call := testCall(`
			select users.id
			from users
			left join lateral (
				select videos.id, videos.user_id
				from videos
			) as v on v.user_id = users.id
			`)
			state := &State{DBInfo: &drivers.DBInfo{
				Tables: []drivers.Table{
					{
						Name: "videos",
						Columns: []drivers.Column{
							{Name: "id"},
							{Name: "user_id"},
						},
					},
				},
			}}
			errs := checkCallWithState(state, call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 28},
				IdentErr{Table: "users", Column: "id", Location: 135},
				IdentErr{Table: "users", Column: "id", Location: 11},
			)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		t.Run("Normal", func(t *testing.T) {
			t.Parallel()

			call := testCall(`update users set name = $1`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 7},
				IdentErr{Column: "name", Location: 17},
			)
		})
		t.Run("Quoted", func(t *testing.T) {
			t.Parallel()

			call := testCall(`update "users" set "name" = $1`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 7},
				IdentErr{Column: "name", Location: 19},
			)
		})
	})

	t.Run("Insert", func(t *testing.T) {
		t.Parallel()

		t.Run("Normal", func(t *testing.T) {
			t.Parallel()

			call := testCall(`insert into users ("id") values ("ok");`)
			errs := checkCallWrapper(call)
			checkErrs(t, errs,
				IdentErr{Table: "users", Location: 12},
				IdentErr{Column: "id", Location: 19},
			)
		})
	})
}

func TestTypeErrors(t *testing.T) {
	t.Parallel()

	state := &State{
		Imports: importers.Collection{
			BasedOnType: map[string]importers.Set{
				"null.Bool": {
					ThirdParty: []string{
						`"github.com/volatiletech/null"`,
					},
				},
			},
		},
		DBInfo: &drivers.DBInfo{
			Tables: []drivers.Table{
				{
					Name: "users",
					Columns: []drivers.Column{
						{
							Name:       "id",
							Type:       "int",
							DBType:     "integer",
							Unique:     true,
							UDTName:    "int4",
							FullDBType: "int4",
						},
						{
							Name:       "name",
							Type:       "string",
							DBType:     "text",
							UDTName:    "text",
							FullDBType: "text",
						},
					},
				},
				{
					Name: "videos",
					Columns: []drivers.Column{
						{
							Name:       "id",
							Type:       "int",
							DBType:     "integer",
							Unique:     true,
							UDTName:    "int4",
							FullDBType: "int4",
						},
						{
							Name:       "video",
							Type:       "string",
							DBType:     "text",
							UDTName:    "text",
							FullDBType: "text",
						},
					},
				},
				{
					Name: "comments",
					Columns: []drivers.Column{
						{
							Name:       "id",
							Type:       "int",
							DBType:     "integer",
							Unique:     true,
							UDTName:    "int4",
							FullDBType: "int4",
						},
						{
							Name:       "comment",
							Type:       "null.String",
							Nullable:   true,
							DBType:     "text",
							UDTName:    "text",
							FullDBType: "text",
						},
					},
				},
			},
		},
	}

	t.Run("Select", func(t *testing.T) {
		t.Parallel()

		call := testCall(`select * from users where id = $1`, "bool")
		errs := checkCallWithState(state, call)
		checkErrs(t, errs,
			TypeErr{Parameter: 1, Column: "id", CallType: "bool", DriverType: "int", DBType: "integer", Location: 31},
		)
	})
}

func checkCallWithState(s *State, fns ...Call) []error {
	return checkCalls(s, fns)
}

func checkCallWrapper(fns ...Call) []error {
	return checkCalls(&State{DBInfo: &drivers.DBInfo{}}, fns)
}

func testCall(sql string, argTypes ...string) Call {
	return Call{
		SQL:      sql,
		ArgTypes: argTypes,
		Package:  "pkg",
		Pos:      token.Position{Filename: "t.go"},
	}
}

func checkIdentErr(t *testing.T, i IdentErr, err error) {
	t.Helper()

	e, ok := err.(IdentErr)
	if !ok {
		t.Errorf("err was not of type UnkIdentErr: %T", err)
		return
	}

	outputErr := func(format string, args ...interface{}) {
		t.Helper()
		t.Errorf("(%s.%s.%s) "+format, append([]interface{}{i.Schema, i.Table, i.Column}, args...)...)
	}

	if i.Kind != 0 && i.Kind != e.Kind {
		outputErr("kind wrong, want: %d, got: %d", i.Kind, e.Kind)
	}
	if len(i.Schema) != 0 && i.Schema != e.Schema {
		outputErr("schema wrong, want: %s, got: %s", i.Schema, e.Schema)
	}
	if len(i.Table) != 0 && i.Table != e.Table {
		outputErr("table wrong, want: %s, got: %s", i.Table, e.Table)
	}
	if len(i.Column) != 0 && i.Column != e.Column {
		outputErr("column wrong, want: %s, got: %s", i.Column, e.Column)
	}
	if i.Location != 0 && i.Location != e.Location {
		outputErr("location wrong, want: %d, got: %d", i.Location, e.Location)
	}
}

func checkTypeErr(t *testing.T, te TypeErr, err error) {
	t.Helper()

	e, ok := err.(TypeErr)
	if !ok {
		t.Errorf("err was not of type TypeErr: %T", err)
		return
	}

	outputErr := func(format string, args ...interface{}) {
		t.Helper()
		t.Errorf("(%s.%s.%s) "+format, append([]interface{}{e.Schema, e.Table, e.Column}, args...)...)
	}

	if len(te.Schema) != 0 && te.Schema != e.Schema {
		outputErr("schema wrong, want: %s, got: %s", te.Schema, e.Schema)
	}
	if len(te.Table) != 0 && te.Table != e.Table {
		outputErr("table wrong, want: %s, got: %s", te.Table, e.Table)
	}
	if len(te.Column) != 0 && te.Column != e.Column {
		outputErr("column wrong, want: %s, got: %s", te.Column, e.Column)
	}
	if len(te.CallType) != 0 && te.CallType != e.CallType {
		outputErr("call type wrong, want: %s, got: %s", te.CallType, e.CallType)
	}
	if len(te.DriverType) != 0 && te.DriverType != e.DriverType {
		outputErr("go type wrong, want: %s, got: %s", te.DriverType, e.DriverType)
	}
	if len(te.DBType) != 0 && te.DBType != e.DBType {
		outputErr("dbtype wrong, want: %s, got: %s", te.DBType, e.DBType)
	}
	if te.Parameter != 0 && te.Parameter != e.Parameter {
		outputErr("parameter wrong, want: %d, got: %d", te.Parameter, e.Parameter)
	}
	if te.Location != 0 && te.Location != e.Location {
		outputErr("location wrong, want: %d, got: %d", te.Location, e.Location)
	}
}

func checkErrs(t *testing.T, errs []error, expect ...error) {
	t.Helper()

	defer func() {
		if !t.Failed() {
			return
		}

		for i, err := range errs {
			t.Errorf("[%d] %s", i, err.Error())
		}
	}()

	if len(expect) != len(errs) {
		t.Errorf("want: %d errors, got: %d", len(expect), len(errs))
		return
	}

	for i, exp := range expect {
		switch expectErr := exp.(type) {
		case IdentErr:
			checkIdentErr(t, expectErr, errs[i])
		case TypeErr:
			checkTypeErr(t, expectErr, errs[i])
		default:
			t.Fatalf("unknown error type found: %T", expectErr)
		}
	}
}

func TestBugNestSelect(t *testing.T) {
	t.Parallel()

	state := &State{
		DBInfo: &drivers.DBInfo{
			Tables: []drivers.Table{
				{
					Name: "tag_videos",
					Columns: []drivers.Column{
						{
							Name:       "tag_id",
							Type:       "int",
							DBType:     "integer",
							Unique:     true,
							UDTName:    "int4",
							FullDBType: "int4",
						},
						{
							Name:       "video_id",
							Type:       "int",
							DBType:     "integer",
							Unique:     true,
							UDTName:    "int4",
							FullDBType: "int4",
						},
					},
				},
			},
		},
	}

	const query = `
		select "tv"."tag_id", (
			select count("tag_videos"."video_id")
			from "tag_videos"
			where "tag_videos"."tag_id" = "tv"."tag_id"
		)
		from "tag_videos" "tv"
		where "tv"."video_id" = $1`

	call := testCall(query, "int")
	errs := checkCallWithState(state, call)

	if len(errs) != 0 {
		t.Error(errs)
	}
}
