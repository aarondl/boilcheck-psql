package main

import (
	"context"
	"database/sql"
	"os"
)

//sqlboiler:check
// this should create a warning since we can't tag constants
var one = `select * from users where user = $1;`

//sqlboiler:check
// the weird path separator is to ensure constants evaluate basic string concats
// correctly
const two = `select * from users;` + string(os.PathSeparator)

const three = `select * from videos;`

const (
	//sqlboiler:check
	// this wrapped in a const block should also be picked up
	four = `select * from tags;`
)

func main() {
	db, err := sql.Open("none", "nothing")
	if err != nil {
		panic(err)
	}

	id := 5
	wrapped := func(a sql.Result, b error) (sql.Result, error) { return a, b }

	// one is a var, we can't use it
	// this would be a false positive if one turned into a constant
	if _, err = db.Exec(one, id); err != nil {
		panic(err)
	}

	//sqlboiler:check
	// This uses an unknowable runtime value and so it should fail
	if _, err = db.Exec(one, id); err != nil {
		panic(err)
	}

	// two should be used because the value is tagged
	// [0]
	if _, err = db.Exec(two, id); err != nil {
		panic(err)
	}

	// two should be used even if wrapped
	// [1]
	if _, err = wrapped(db.Exec(two, sql.NullBool{Bool: false, Valid: true})); err != nil {
		panic(err)
	}

	//sqlboiler:check
	// [2] three should be able to be checked because it's a constant
	_, err = wrapped(db.Exec(three, id))
	if err != nil {
		panic(err)
	}

	//sqlboiler:check
	// [3] inline constant, no assignment
	db.Exec(`select * from logs;`, sql.NullBool{
		Bool:  false,
		Valid: true,
	})

	// QueryRow into scan
	var queryRowScan int
	//sqlboiler:check
	// [4]
	err = db.QueryRow(`select * from logs where id = $1;`, id).Scan(&queryRowScan)
	if err != nil {
		panic(err)
	}

	// QueryRow into scan from a checked constant
	// [5]
	db.QueryRowContext(context.Background(), two, id).Scan(&queryRowScan)

	//sqlboiler:check
	// [6] inline constant expression, no assignment
	db.Exec(`select * from `+`users;`, id)

	// [7] block const () declared
	db.Exec(four, id)

	//sqlboiler:check
	// scoped constant
	const five = `select * from comments;`

	const (
		//sqlboiler:check
		// scoped block declaration
		six = `select * from logins;`
	)

	// [8] using scoped constant
	if _, err = db.Exec(five, id); err != nil {
		panic(err)
	}

	// [9] using scoped constant
	db.Exec(six, id)
}
