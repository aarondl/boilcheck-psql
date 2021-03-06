package main

import (
	"fmt"
	"path"
	"strings"

	"github.com/volatiletech/sqlboiler/v4/drivers"

	pgquery "github.com/lfittl/pg_query_go"
	pgnodes "github.com/lfittl/pg_query_go/nodes"
)

// Types of errors
const (
	Unknown = iota
	Ambiguous

	unknownTypeSentinel = "UNKNOWNTYPESENTINEL"
)

// IdentErr is an unknown identifier error that occurs when the database
// does not contain information that proves the identifiers existence.
type IdentErr struct {
	// Kind is Unknown/Ambiguous
	Kind int

	Schema   string
	Table    string
	Column   string
	Location int

	Fn Call
}

func (i IdentErr) Error() string {
	lnt, lnc := len(i.Table), len(i.Column)
	var ident string
	switch {
	case lnt != 0 && lnc != 0:
		ident = i.Table + "." + i.Column
	case lnc != 0:
		ident = i.Column
	default:
		ident = i.Table
	}

	if len(i.Schema) != 0 && i.Schema != "public" {
		ident = i.Schema + "." + ident
	}

	var errMsg string
	switch i.Kind {
	case Ambiguous:
		errMsg = "ambiguous identifier in sql statement"
	case Unknown:
		errMsg = "unknown identifier in sql statement"
	}

	return fmt.Sprintf("%s:%d:%d %s: %s at pos %d",
		i.Fn.Pos.Filename,
		i.Fn.Pos.Line,
		i.Fn.Pos.Column,
		errMsg,
		ident,
		i.Location,
	)
}

// TypeErr occurs when the function arguments given do not match the
// parameters.
type TypeErr struct {
	Schema string
	Table  string
	Column string

	CallType   string
	DriverType string
	DBType     string

	Parameter int
	Location  int

	Fn Call
}

func (t TypeErr) Error() string {
	ident := t.Column
	if len(t.Table) != 0 {
		ident = t.Table + "." + ident
	}
	if len(t.Schema) != 0 && t.Schema != "public" {
		ident = t.Schema + "." + ident
	}

	return fmt.Sprintf("%s:%d:%d type mismatch, %q has type %q (db: %s) but parameter $%d (pos %d) is %q",
		t.Fn.Pos.Filename,
		t.Fn.Pos.Line,
		t.Fn.Pos.Column,
		ident,
		t.DriverType,
		t.DBType,
		t.Parameter,
		t.Location,
		t.CallType,
	)
}

// ParseError occurs when a statement fails to parse
type ParseError struct {
	Err error
	Fn  Call
}

func (p ParseError) Error() string {
	return fmt.Sprintf("%s:%d:%d parse error: %v",
		p.Fn.Pos.Filename,
		p.Fn.Pos.Line,
		p.Fn.Pos.Column,
		p.Err,
	)
}

func checkCalls(state *State, fns []Call) (errs []error) {
	for _, fn := range fns {
		tree, err := pgquery.Parse(fn.SQL)
		if err != nil {
			errs = append(errs, ParseError{Err: err, Fn: fn})
		}

		errs = append(errs, checkCall(state, fn, tree)...)
	}

	return errs
}

func checkCall(state *State, fn Call, tree pgquery.ParsetreeList) (errs []error) {
	for _, stmt := range tree.Statements {
		// Quite often things are packed in the raw statement
		if raw, ok := stmt.(pgnodes.RawStmt); ok {
			stmt = raw.Stmt
		}

		// Create a scope for each statement we parse as they should be separate
		errList := checkCallRecurse(state, fn, NewScope(state.DBInfo), stmt)
		if len(errList) != 0 {
			errs = append(errs, errList...)
		}
	}

	return errs
}

// checkCallRecurse looks through a parsed sql node and searches for missing
// identifiers or type mismatches.
//
// The scope is a set of in-scope identifiers.
func checkCallRecurse(state *State, fn Call, scope *Scope, n pgnodes.Node) (errs []error) {
	descend := func(node pgnodes.Node) []error {
		return append(errs, checkCallRecurse(state, fn, scope, node)...)
	}

	switch node := n.(type) {
	case pgnodes.RawStmt:
		// Rawstmt seems to be the root of most expressions
		panic("there should be no raw statements at this level")
	case pgnodes.SelectStmt:
		_, errList := checkSelect(state, fn, scope, node)
		errs = append(errs, errList...)
	case pgnodes.UpdateStmt:
		errs = append(errs, checkUpdate(state, fn, scope, node)...)
	case pgnodes.InsertStmt:
		errs = append(errs, checkInsert(state, fn, scope, node)...)
	case pgnodes.DeleteStmt:
		errs = append(errs, checkDelete(state, fn, scope, node)...)
	case pgnodes.SortBy:
		errs = descend(node.Node)
	case pgnodes.FuncCall:
		for _, arg := range node.Args.Items {
			errs = descend(arg)
		}
	case pgnodes.A_Expr:
		errs = descend(node.Lexpr)
		errs = descend(node.Rexpr)

		if err := typeCheck(state, fn, scope, node.Lexpr, node.Rexpr); err != nil {
			errs = append(errs, err)
		}
	case pgnodes.BoolExpr:
		for _, i := range node.Args.Items {
			errs = descend(i)
		}
	case pgnodes.ColumnRef:
		offset := 0

		var schema, table string
		if len(node.Fields.Items) == 3 {
			schema = node.Fields.Items[0].(pgnodes.String).Str
			table = node.Fields.Items[1].(pgnodes.String).Str
			offset += 2
		} else if len(node.Fields.Items) == 2 {
			table = node.Fields.Items[0].(pgnodes.String).Str
			offset += 1
		}

		switch item := node.Fields.Items[offset].(type) {
		case pgnodes.String:
			column := item.Str
			var kind int
			ret := scope.has(schema, table, column)
			switch ret {
			case scopeRetUnknown:
				kind = Unknown
			case scopeRetAmbiguous:
				kind = Ambiguous
			}

			if ret != scopeRetOk {
				errs = append(errs, IdentErr{
					Kind:     kind,
					Schema:   schema,
					Table:    table,
					Column:   column,
					Location: node.Location,
					Fn:       fn,
				})
			}
		case pgnodes.A_Star:
			break
		default:
			panic(fmt.Sprintf("%T", node.Fields.Items[1]))
		}
	case pgnodes.SubLink:
		errs = descend(node.Subselect)
	case pgnodes.ResTarget:
		// ResTarget can also happen in Select lists, but we circumvent
		// that in the select case
		if node.Name != nil {
			col := *node.Name
			// Ambiguous can't happen because there's only one table allowed
			// in an update statement.
			if scope.has("", "", col) == scopeRetUnknown {
				errs = append(errs, IdentErr{
					Column:   col,
					Location: node.Location,
					Fn:       fn,
				})
			}
		}
		if node.Val != nil {
			errs = descend(node.Val)
		}
	}

	return errs
}

func checkSelect(state *State, fn Call, scope *Scope, sel pgnodes.SelectStmt) (outputCallRefs []outputColRef, errs []error) {
	descend := func(node pgnodes.Node) []error {
		return append(errs, checkCallRecurse(state, fn, scope, node)...)
	}

	// If this is an "upper level select" then lets just check the
	// selects themselves as separate entities.
	if sel.Larg != nil && sel.Rarg != nil {
		errs = descend(*sel.Larg)
		errs = descend(*sel.Rarg)
		return nil, errs
	}

	// Bring all the tables into scope
	nTables := 0

	addTable := func(r pgnodes.RangeVar) {
		table := *r.Relname
		var alias, schema string
		if r.Schemaname != nil {
			schema = *r.Schemaname
		}
		if r.Alias != nil {
			alias = *r.Alias.Aliasname
		}

		if !scope.pushTable(schema, table, alias) {
			errs = append(errs, IdentErr{
				Schema:   schema,
				Table:    table,
				Location: r.Location,
				Fn:       fn,
			})
		} else {
			nTables++
		}
	}

	// The joins are recursive in nature, but we're going to
	// process it iteratively since we're still in the middle
	// of processing a select statement
	stack := make([]pgnodes.Node, len(sel.FromClause.Items))
	copy(stack, sel.FromClause.Items)

	for len(stack) > 0 {
		popped := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		switch item := popped.(type) {
		case pgnodes.RangeVar:
			addTable(item)
		case pgnodes.JoinExpr:
			stack = append(stack, item.Quals)
			stack = append(stack, item.Rarg)
			stack = append(stack, item.Larg)
		case pgnodes.A_Expr:
			errs = descend(popped)
		case pgnodes.BoolExpr:
			errs = descend(popped)
		case pgnodes.RangeSubselect:
			if item.Alias == nil {
				panic(fmt.Sprintf("subquery: %s\nmust have alias", item.Subquery.Deparse()))
			}

			var subSelectScope *Scope
			if item.Lateral {
				// Lateral things have access to the outside world
				// but we don't want to poison our current scope with
				// its internal nonsense so clone it
				subSelectScope = scope.clone()
			} else {
				subSelectScope = NewScope(scope.info)
			}

			subSelect, ok := item.Subquery.(pgnodes.SelectStmt)
			if !ok {
				panic(fmt.Sprintf("subquery: %s\nnot a select statement", item.Subquery.Deparse()))
			}

			subSelMap, subSelectErrs := checkSelect(state, fn, subSelectScope, subSelect)
			if len(subSelectErrs) > 0 {
				errs = append(errs, subSelectErrs...)
			}

			pseudoTable := outputColsToPseudoTable(*item.Alias.Aliasname, subSelMap)
			scope.pushPseudoTable(*item.Alias.Aliasname, pseudoTable)
			nTables++
		default:
			panic(fmt.Sprintf("what is this weird from statement: %T", item))
		}
	}

	// Follow-up clauses
	errs = descend(sel.WhereClause)
	errs = descend(sel.HavingClause)

	// Process select list after where/having, but before GroupBy and
	// OrderBy so that we can create a list of output_name's that
	// can be referenced by those two clauses.
	//
	// Processing in this way also stops the ResTarget case from
	// seeing these aliases and attempting to resolve them as real names
	// as in the update clause case.
	var addRefs []outputColRef
	for _, listItem := range sel.TargetList.Items {
		resTarg := listItem.(pgnodes.ResTarget)

		var name string
		if resTarg.Name != nil {
			name = *resTarg.Name
		}

		var column *drivers.Column
		colRef, ok := resTarg.Val.(pgnodes.ColumnRef)
		if ok {
			var schema, table, col string
			ln := len(colRef.Fields.Items)

			if ln == 1 {
				if _, ok := colRef.Fields.Items[0].(pgnodes.A_Star); ok {
					panic("subqueries with * are not supported yet")
				}
			}

			col = colRef.Fields.Items[ln-1].(pgnodes.String).Str
			if ln >= 2 {
				table = colRef.Fields.Items[ln-2].(pgnodes.String).Str
			}
			if ln >= 3 {
				schema = colRef.Fields.Items[ln-3].(pgnodes.String).Str
			}

			var ret int
			column, ret = scope.get(schema, table, col)
			if ret != scopeRetOk {
				errs = append(errs, IdentErr{
					Schema:   schema,
					Table:    table,
					Column:   col,
					Location: colRef.Location,
					Fn:       fn,
				})
				continue
			}

			if len(name) == 0 {
				name = column.Name
			}
		}

		addRefs = append(addRefs, outputColRef{
			name: name, col: column,
		})
	}

	for _, r := range addRefs {
		scope.pushOutputName(r.name, r.col)
	}

	for _, items := range sel.GroupClause.Items {
		errs = descend(items)
	}
	for _, items := range sel.SortClause.Items {
		errs = descend(items)
	}

	for range addRefs {
		scope.popOutputName()
	}

	for i := 0; i < nTables; i++ {
		scope.popTable()
	}

	return addRefs, errs
}

func checkUpdate(state *State, fn Call, scope *Scope, update pgnodes.UpdateStmt) (errs []error) {
	var schema, alias string
	if update.Relation.Schemaname != nil {
		schema = *update.Relation.Schemaname
	}
	if update.Relation.Alias != nil {
		alias = *update.Relation.Alias.Aliasname
	}
	table := *update.Relation.Relname

	nTables := 0
	if !scope.pushTable(schema, table, alias) {
		errs = append(errs, IdentErr{
			Schema:   schema,
			Table:    table,
			Location: update.Relation.Location,
			Fn:       fn,
		})
	} else {
		nTables++
	}

	for _, c := range update.TargetList.Items {
		errs = append(errs, checkCallRecurse(state, fn, scope, c)...)
	}
	errs = append(errs, checkCallRecurse(state, fn, scope, update.WhereClause)...)

	for i := 0; i < nTables; i++ {
		scope.popTable()
	}

	return errs
}

func checkInsert(state *State, fn Call, scope *Scope, ins pgnodes.InsertStmt) (errs []error) {
	var schema, alias string
	if ins.Relation.Schemaname != nil {
		schema = *ins.Relation.Schemaname
	}
	if ins.Relation.Alias != nil {
		alias = *ins.Relation.Alias.Aliasname
	}
	table := *ins.Relation.Relname

	nTables := 0
	if !scope.pushTable(schema, table, alias) {
		errs = append(errs, IdentErr{
			Schema:   schema,
			Table:    table,
			Location: ins.Relation.Location,
			Fn:       fn,
		})
	} else {
		nTables++
	}

	for _, c := range ins.Cols.Items {
		errs = append(errs, checkCallRecurse(state, fn, scope, c)...)
	}

	for i := 0; i < nTables; i++ {
		scope.popTable()
	}

	return errs
}

func checkDelete(state *State, fn Call, scope *Scope, del pgnodes.DeleteStmt) (errs []error) {
	var schema, alias string
	if del.Relation.Schemaname != nil {
		schema = *del.Relation.Schemaname
	}
	if del.Relation.Alias != nil {
		alias = *del.Relation.Alias.Aliasname
	}
	table := *del.Relation.Relname

	nTables := 0
	if !scope.pushTable(schema, table, alias) {
		errs = append(errs, IdentErr{
			Schema:   schema,
			Table:    table,
			Location: del.Relation.Location,
			Fn:       fn,
		})
	} else {
		nTables++
	}

	errs = append(errs, checkCallRecurse(state, fn, scope, del.WhereClause)...)

	for i := 0; i < nTables; i++ {
		scope.popTable()
	}

	return errs
}

func typeCheck(s *State, fn Call, scope *Scope, lhs, rhs pgnodes.Node) error {
	if lhs == nil || rhs == nil {
		return nil
	}

	var c *pgnodes.ColumnRef
	var p *pgnodes.ParamRef

	if col, ok := lhs.(pgnodes.ColumnRef); ok {
		c = &col
	}
	if col, ok := rhs.(pgnodes.ColumnRef); ok {
		c = &col
	}

	if param, ok := lhs.(pgnodes.ParamRef); ok {
		p = &param
	}
	if param, ok := rhs.(pgnodes.ParamRef); ok {
		p = &param
	}

	if c == nil || p == nil {
		return nil
	}

	offset := 0

	var schema, table string
	if len(c.Fields.Items) == 3 {
		schema = c.Fields.Items[0].(pgnodes.String).Str
		table = c.Fields.Items[1].(pgnodes.String).Str
		offset += 2
	} else if len(c.Fields.Items) == 2 {
		table = c.Fields.Items[0].(pgnodes.String).Str
		offset += 1
	}
	var column string

	switch item := c.Fields.Items[offset].(type) {
	case pgnodes.String:
		column = item.Str
	default:
		panic(fmt.Sprintf("type check against weird node type: %T", c.Fields.Items[1]))
	}

	col, ret := scope.get(schema, table, column)
	if ret != scopeRetOk {
		// It's the job of a different function to do unknown/ambiguous errors
		// it's unfortunate that scope.get will be called again but it's less
		// bad than duplicating the errors.
		return nil
	} else if col == nil {
		// This is possible even with a scopeRetOk in the odd case where someone
		// aliased an expression in a select statement (select 't' as hello)
		// and then used that in a groupby/orderby in some expression involving
		// a parameter.
		return nil
	}

	if p.Number-1 >= len(fn.ArgTypes) {
		return TypeErr{
			Schema:     schema,
			Table:      table,
			Column:     column,
			CallType:   "<none>",
			DriverType: col.Type,
			DBType:     col.DBType,
			Parameter:  p.Number,
			Location:   p.Location,
			Fn:         fn,
		}
	}
	// argType is something like database/sql.NullBool or int
	argType := fn.ArgTypes[p.Number-1]

	// We need to normalize our type to be equivalent to argType
	normalizedType := col.Type
	if splits := strings.Split(col.Type, "."); len(splits) > 1 {
		// This is a type from a package, try to resolve it
		imports := s.Imports.BasedOnType[col.Type]
		var imp string

		allImps := make([]string, len(imports.Standard)+len(imports.ThirdParty))
		copy(allImps, imports.Standard)
		copy(allImps[len(imports.Standard):], imports.ThirdParty)

		for _, i := range allImps {
			noQuotes := strings.Trim(i, `"`)
			if splits[1] != path.Base(noQuotes) {
				continue
			}

			packageDir := path.Dir(noQuotes)
			imp = path.Join(packageDir, col.Type)
			break
		}

		if len(imp) == 0 {
			return fmt.Errorf("failed to lookup package for driver type: %s", col.Type)
		}
	}

	if argType != normalizedType {
		return TypeErr{
			Schema:     schema,
			Table:      table,
			Column:     column,
			CallType:   argType,
			DriverType: col.Type,
			DBType:     col.DBType,
			Parameter:  p.Number,
			Location:   p.Location,
			Fn:         fn,
		}
	}

	return nil
}

// Scope keeps track of tables that are in scope (and transitively the columns
// that are in scope).
type Scope struct {
	// The DB Info to check against
	// when adding something to scope
	info *drivers.DBInfo

	// The objects in scope
	tables      []*drivers.Table
	aliases     []string
	outputNames []outputColRef
}

type outputColRef struct {
	name string
	col  *drivers.Column
}

func outputColsToPseudoTable(name string, refs []outputColRef) *drivers.Table {
	table := &drivers.Table{Name: name}

	for _, r := range refs {
		var col drivers.Column
		if r.col != nil {
			col = *r.col
		}
		col.Name = r.name

		table.Columns = append(table.Columns, col)
	}

	return table
}

// NewScope creates a new object for keeping track of tables in scope
func NewScope(info *drivers.DBInfo) *Scope {
	return &Scope{
		info: info,
	}
}

// clone the object so it can continue to be used without affecting the parent
// scope
func (s *Scope) clone() *Scope {
	cloned := new(Scope)
	cloned.info = s.info
	cloned.tables = make([]*drivers.Table, len(s.tables))
	cloned.aliases = make([]string, len(s.aliases))
	cloned.outputNames = make([]outputColRef, len(s.outputNames))
	copy(cloned.tables, s.tables)
	copy(cloned.aliases, s.aliases)
	copy(cloned.outputNames, s.outputNames)
	return cloned
}

// pushTable adds the table to the current scope. If it fails that means
// the database info did not contain that table.
func (s *Scope) pushTable(schema, table, alias string) bool {
	debugf("PUSH: s(%s) t(%s) a(%s)\n", schema, table, alias)
	for i, t := range s.info.Tables {
		if len(schema) != 0 {
			if t.SchemaName != schema {
				continue
			}
		}

		if t.Name == table {
			s.aliases = append(s.aliases, alias)
			s.tables = append(s.tables, &s.info.Tables[i])
			return true
		}
	}

	return false
}

// pushPseudoTable is used for when we need to create a new table
// out of thin air. Useful for subqueries etc.
func (s *Scope) pushPseudoTable(alias string, data *drivers.Table) {
	debugf("PUSH(P): a(%s)\n", alias)
	s.aliases = append(s.aliases, alias)
	s.tables = append(s.tables, data)
}

func (s *Scope) popTable() {
	debugf("POP: t(%s) a(%s)\n", s.tables[len(s.tables)-1].Name, s.aliases[len(s.aliases)-1])
	s.aliases = s.aliases[:len(s.aliases)-1]
	s.tables = s.tables[:len(s.tables)-1]
}

func (s *Scope) pushOutputName(name string, col *drivers.Column) {
	s.outputNames = append(s.outputNames, outputColRef{name: name, col: col})
}

func (s *Scope) popOutputName() {
	s.outputNames = s.outputNames[:len(s.outputNames)-1]
}

const (
	scopeRetOk = iota
	scopeRetAmbiguous
	scopeRetUnknown
)

// get can return a nil column in the case of groupby/orderby clauses
// that are using expressions
func (s *Scope) get(schema, table, column string) (*drivers.Column, int) {
	if len(table) != 0 {
		// Providing a table name means we know exactly what we're looking for
		// and if it's something we've aliased even more so.
		var inScope *drivers.Table
		for i, t := range s.tables {
			if s.aliases[i] == table {
				inScope = t
				break
			}

			if len(schema) != 0 && t.SchemaName != schema {
				continue
			}

			if t.Name == table {
				inScope = t
				break
			}
		}

		tname := ""
		if inScope != nil {
			tname = inScope.Name
		}
		debugf("GET: s(%s) t(%s) c(%s)\t[%t]=[%s]\n", schema, table, column, inScope == nil, tname)

		// Could not find the table they referred to
		if inScope == nil {
			return nil, scopeRetUnknown
		}

		for i, c := range inScope.Columns {
			if c.Name == column {
				return &inScope.Columns[i], scopeRetOk
			}
		}

		return nil, scopeRetUnknown
	}

	// They did not provide a table name at all, so we're going to have to
	// search for the name. We cannot break early because we have to also detect
	// if there's an ambiguous identifier
	ret := scopeRetUnknown

	var col *drivers.Column
	for _, inScope := range s.tables {
		for i, c := range inScope.Columns {
			if c.Name == column {
				if ret == scopeRetOk {
					return nil, scopeRetAmbiguous
				}
				col = &inScope.Columns[i]
				ret = scopeRetOk
			}
		}
	}

	// Finally check the outputNames to see if the column identifier is there
	for _, o := range s.outputNames {
		if column == o.name {
			return o.col, scopeRetOk
		}
	}

	return col, ret
}

func (s *Scope) has(schema, table, column string) int {
	_, ret := s.get(schema, table, column)
	return ret
}
