package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Call is a whitelisted function call (like sql.DB.Exec) that occurs in a
// Go source file
type Call struct {
	SQL      string
	ArgTypes []string

	Package string
	Pos     token.Position
}

// Constant declaration in Go
type Constant struct {
	Name    string
	Val     string
	ValSpec *ast.ValueSpec
	Pos     token.Position
}

// Warn user of a misuse of the program at some line
type Warn struct {
	Err string
	Pos token.Position
}

func (w Warn) Error() string {
	return fmt.Sprintf("%s:%d:%d %s", w.Pos.Filename, w.Pos.Line, w.Pos.Column, w.Err)
}

func findTaggedCalls(pkgs []*packages.Package) (calls []Call, warns []Warn) {
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			commentMap := ast.NewCommentMap(pkg.Fset, file, file.Comments)
			consts, fileCalls, fileWarns := iterateCommentMap(pkg, commentMap)

			moreCalls, moreWarns := tagCallsByConstant(pkg, file, consts)
			fileCalls = append(fileCalls, moreCalls...)
			fileWarns = append(fileWarns, moreWarns...)

			for i := range fileCalls {
				fileCalls[i].Package = pkg.PkgPath
			}

			calls = append(calls, fileCalls...)
			warns = append(warns, fileWarns...)
		}
	}

	sort.Slice(calls, func(i, j int) bool {
		ci, cj := calls[i], calls[j]
		if ci.Package < cj.Package {
			return true
		} else if ci.Package > cj.Package {
			return false
		}

		if ci.Pos.Filename < cj.Pos.Filename {
			return true
		} else if ci.Pos.Filename > cj.Pos.Filename {
			return false
		}

		if ci.Pos.Line < cj.Pos.Line {
			return true
		} else if ci.Pos.Line > cj.Pos.Line {
			return false
		}

		if ci.Pos.Column < cj.Pos.Column {
			return true
		} else if ci.Pos.Column > cj.Pos.Column {
			return false
		}

		return false
	})
	sort.Slice(warns, func(i, j int) bool {
		ci, cj := warns[i], warns[j]
		if ci.Pos.Filename < cj.Pos.Filename {
			return true
		} else if ci.Pos.Filename > cj.Pos.Filename {
			return false
		}

		if ci.Pos.Line < cj.Pos.Line {
			return true
		} else if ci.Pos.Line > cj.Pos.Line {
			return false
		}

		if ci.Pos.Column < cj.Pos.Column {
			return true
		} else if ci.Pos.Column > cj.Pos.Column {
			return false
		}

		return false
	})

	return calls, warns
}

func iterateCommentMap(pkg *packages.Package, cm ast.CommentMap) ([]Constant, []Call, []Warn) {
	var consts []Constant
	var calls []Call
	var warns []Warn

	for node, comments := range cm {
		found := false

		for _, c := range comments {
			if strings.HasPrefix(c.Text(), "sqlboiler:check") {
				found = true
				break
			}
		}

		if !found {
			continue
		}

		// Declaration statements occur inside function scopes
		// and simply contain a GenDecl, whereas GenDecl occurs at
		// the top level of the file.
		//
		// It's also possible to have a ValueSpec nested inside a GenDecl
		// which will not be pointed to by the file's comments
		genNode := node
		if declStmt, ok := genNode.(*ast.DeclStmt); ok {
			genNode = declStmt.Decl
		}
		if genDec, ok := genNode.(*ast.GenDecl); ok {
			c, w := tagConstants(pkg, genDec)
			consts = append(consts, c...)
			warns = append(warns, w...)
			continue
		}
		if valSpec, ok := genNode.(*ast.ValueSpec); ok {
			c, w := tagValueSpecConstants(pkg, valSpec)
			consts = append(consts, c...)
			warns = append(warns, w...)
			continue
		}

		// If it's not a GenDecl, try to find a call in the tagged expression
		call, err := tagCall(pkg, node)
		if err != nil {
			warns = append(warns, Warn{
				Err: err.Error(),
				Pos: pkg.Fset.Position(node.Pos()),
			})
		}

		if call == nil {
			warns = append(warns, Warn{
				Err: "failed to find either function or constant after sqlboiler:check tag",
				Pos: pkg.Fset.Position(node.Pos()),
			})
			continue
		}

		calls = append(calls, *call)
	}

	return consts, calls, warns
}

// tagConstants
func tagConstants(pkg *packages.Package, genDec *ast.GenDecl) (consts []Constant, warns []Warn) {
	if genDec.Tok != token.CONST {
		warns = append(warns, Warn{
			Err: "tagged declaration was not a constant",
			Pos: pkg.Fset.Position(genDec.Pos()),
		})
		return nil, warns
	}

	for _, spec := range genDec.Specs {
		// This is safe because the Tok check above guarantees spec's type
		// to be ValueSpec
		valSpec := spec.(*ast.ValueSpec)
		moreConsts, moreWarns := tagValueSpecConstants(pkg, valSpec)
		consts = append(consts, moreConsts...)
		warns = append(warns, moreWarns...)
	}

	return consts, warns
}

func tagValueSpecConstants(pkg *packages.Package, valSpec *ast.ValueSpec) (consts []Constant, warns []Warn) {
	for i, name := range valSpec.Names {
		if name.Obj != nil && name.Obj.Kind.String() != "const" {
			warns = append(warns, Warn{
				Err: "tagged declaration had obj pointer but was not const",
				Pos: pkg.Fset.Position(name.Pos()),
			})
			continue
		}

		if name.Name == "_" {
			warns = append(warns, Warn{
				Err: "tagged declaration assigned to blank identifier",
				Pos: pkg.Fset.Position(name.Pos()),
			})
			continue
		}

		typeVal := pkg.TypesInfo.Types[valSpec.Values[i]]
		if !typeVal.IsValue() {
			warns = append(warns, Warn{
				Err: "could not determine type for tagged declaration",
				Pos: pkg.Fset.Position(name.Pos()),
			})
			continue
		}

		consts = append(consts, Constant{
			Name:    name.Name,
			Val:     constant.StringVal(typeVal.Value),
			ValSpec: valSpec,
			Pos:     pkg.Fset.Position(valSpec.Pos()),
		})
	}

	return consts, warns
}

type sqlFunction struct {
	Name       string
	HasContext bool
}

var functionWhitelist = []sqlFunction{
	{Name: "Exec", HasContext: false},
	{Name: "ExecContext", HasContext: true},
	{Name: "Query", HasContext: false},
	{Name: "QueryContext", HasContext: true},
	{Name: "QueryRow", HasContext: false},
	{Name: "QueryRowContext", HasContext: true},
	{Name: "SQL", HasContext: false},
}

// tagCallsByConstant iterates through the entire package AST and looks
// for function calls. If they match the function whitelist AND it's sql
// argument is a tagged constant then it too becomes tagged.
func tagCallsByConstant(pkg *packages.Package, file *ast.File, consts []Constant) (calls []Call, warns []Warn) {
	var walkFn visitorFn
	walkFn = visitorFn(func(node ast.Node) ast.Visitor {
		if node == nil {
			return nil
		}

		callExpr, ok := node.(*ast.CallExpr)
		if !ok {
			return walkFn
		}

		// Check the arguments of the function for a constant we know about
		var constVal *Constant
		var constIndex int
		for i, argExpr := range callExpr.Args {
			// If this arg is not an identifier keep searching
			ident, ok := argExpr.(*ast.Ident)
			if !ok {
				continue
			}

			// See if this arg is a constant we know of
			for _, c := range consts {
				if c.Name != ident.Name {
					continue
				}

				vspec, ok := ident.Obj.Decl.(*ast.ValueSpec)
				if !ok {
					continue
				}

				if vspec != c.ValSpec {
					continue
				}

				constVal = &c
				constIndex = i
				break
			}
		}

		if constVal == nil {
			// We did not find a usage of one of the constants, keep walkin'
			return walkFn
		}

		// Check if this is a whitelisted func
		_, _, fn := getSQLFunction(callExpr)
		if fn == nil {
			// This function consumes a tagged argument
			// but is not whitelisted, flag this as a problem
			warns = append(warns, Warn{
				Err: "tagged constant used in non-sql function",
				Pos: pkg.Fset.Position(callExpr.Args[constIndex].Pos()),
			})
			return walkFn
		}

		// We would have already skipped over the function's ctx arg
		// so we should simply be able to get the rest of them
		argTypes := make([]string, 0, len(callExpr.Args))
		for i := constIndex + 1; i < len(callExpr.Args); i++ {
			arg := callExpr.Args[i]
			typeAndVal, ok := pkg.TypesInfo.Types[arg]
			if !ok {
				warns = append(warns, Warn{
					Err: fmt.Sprintf("argument %d type unknown", i),
					Pos: pkg.Fset.Position(arg.Pos()),
				})
				// Continue walking, we can't record this function
				return walkFn
			}

			argTypes = append(argTypes, typeAndVal.Type.String())
		}

		calls = append(calls, Call{
			SQL:      constVal.Val,
			ArgTypes: argTypes,
			Pos:      pkg.Fset.Position(callExpr.Pos()),
		})

		return nil
	})

	ast.Walk(walkFn, file)
	return calls, warns
}

type visitorFn func(node ast.Node) ast.Visitor

func (vfn visitorFn) Visit(node ast.Node) ast.Visitor {
	return vfn(node)
}

// tagCall drills down into a tagged AST node and finds a function call
// that we care about.
//
// It returns nil, nil if no function wanted to be found (eg a node type that
// we cannot find a function call in was passed in)
//
// It returns nil, err if there was a problem looking up the function/it's args
// because the user clearly intended us to find a function call we could use
// but we couldn't.
func tagCall(pkg *packages.Package, node ast.Node) (call *Call, err error) {
	// Don't process const/var decls in this function
	if _, ok := node.(*ast.GenDecl); ok {
		return nil, nil
	}

	currentNode := node
Loop:
	for currentNode != nil {
		switch n := currentNode.(type) {
		case *ast.CallExpr:
			_, _, fn := getSQLFunction(n)

			if fn == nil {
				// It's also possible that we're in a function call but the
				// selector is itself another function call db.QueryRow().Scan()
				// so we can check for this
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					if ce, ok := sel.X.(*ast.CallExpr); ok {
						currentNode = ce
						continue Loop
					}
				}

				// It's possible that there's a wrapping function, descend
				// into the functions arguments, if we find a call stop
				// processing
				for _, fnArg := range n.Args {
					// Some arguments will likely be random nonsense we don't
					// care about, don't worry about those, but if we get our
					// call back just return.
					call, _ := tagCall(pkg, fnArg)
					if call != nil {
						return call, nil
					}
				}

				break Loop
			}

			sqlOffset := 0
			if fn.HasContext {
				sqlOffset = 1
			}

			var sql string
			switch arg := n.Args[sqlOffset].(type) {
			case *ast.Ident:
				if arg.Obj.Kind != ast.Con {
					// The sql argument is an identifier, but not one
					// that points to a const
					return nil, Warn{
						Err: fmt.Sprintf("argument %q to sql function is not a constant", arg.Name),
						Pos: pkg.Fset.Position(arg.Pos()),
					}
				}

				switch decl := arg.Obj.Decl.(type) {
				case *ast.ValueSpec:
					typeVal, ok := pkg.TypesInfo.Types[decl.Values[0]]
					if !ok || !typeVal.IsValue() {
						return nil, Warn{
							Err: "could not find string value for sql statement",
							Pos: pkg.Fset.Position(decl.Pos()),
						}
					}

					sql = constant.StringVal(typeVal.Value)
				default:
					return nil, Warn{
						Err: fmt.Sprintf("declaration of %q is not a value", arg.Name),
						Pos: pkg.Fset.Position(arg.Obj.Pos()),
					}
				}
			default:
				typeVal, ok := pkg.TypesInfo.Types[arg]
				if !ok || !typeVal.IsValue() {
					return nil, Warn{
						Err: "sql argument to function is not an identifier or a constant string",
						Pos: pkg.Fset.Position(arg.Pos()),
					}
				}

				sql = constant.StringVal(typeVal.Value)
			}

			var argTypes []string
			for i := sqlOffset + 1; i < len(n.Args); i++ {
				arg := n.Args[i]

				typeAndVal, ok := pkg.TypesInfo.Types[arg]
				if !ok {
					return nil, Warn{
						Err: fmt.Sprintf("argument %d type unknown", i+1),
						Pos: pkg.Fset.Position(arg.Pos()),
					}
				}

				argTypes = append(argTypes, typeAndVal.Type.String())
			}

			return &Call{
				SQL:      sql,
				ArgTypes: argTypes,
				Pos:      pkg.Fset.Position(n.Pos()),
			}, nil
		case *ast.ExprStmt:
			// When its not assigned to anything
			currentNode = n.X
		case *ast.IfStmt:
			// When it's assigned inside an if statement
			currentNode = n.Init
		case *ast.AssignStmt:
			// When it's returns are assigned to variables
			currentNode = n.Rhs[0]
		default:
			currentNode = nil
		}
	}

	return nil, nil
}

func getSQLFunction(expr *ast.CallExpr) (string, token.Pos, *sqlFunction) {
	var name string
	var pos token.Pos
	switch id := expr.Fun.(type) {
	case *ast.Ident:
		name = id.Name
		pos = id.Pos()
	case *ast.SelectorExpr:
		name = id.Sel.Name
		pos = id.Pos()
	default:
		// Not sure how to handle this case
		panic("unknown function call name type")
	}

	var fn *sqlFunction
	for _, whitelisted := range functionWhitelist {
		if whitelisted.Name == name {
			fn = &whitelisted
			break
		}
	}

	return name, pos, fn
}
