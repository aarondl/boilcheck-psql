package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/importers"

	"github.com/BurntSushi/toml"
	"github.com/friendsofgo/errors"
	"golang.org/x/tools/go/packages"
)

var (
	flagDir     string
	flagConfig  string
	flagDriver  string
	flagVerbose bool
	flagDebug   bool
)

var (
	dbInfo *drivers.DBInfo
)

// State of the application
type State struct {
	DBInfo      *drivers.DBInfo
	Imports     importers.Collection
	TypeAliases map[string][]string
}

func main() {
	// Setup flags
	flag.StringVar(&flagDir, "dir", ".", "The dir to search for Go files")
	flag.StringVar(&flagConfig, "config", "sqlboiler.toml", "The config file to load")
	flag.StringVar(&flagDriver, "driver", "psql", "The driver binary")
	flag.BoolVar(&flagVerbose, "verbose", false, "Verbose output")
	flag.BoolVar(&flagDebug, "debug", false, "Turn on debugging output")
	flag.Parse()

	// Init the app
	initDriver(flagDriver)
	cfg, err := loadConfig(flagConfig)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to initialize config:", err)
		os.Exit(1)
	}

	pkgs, err := loadPackages(flagDir, flag.Args()...)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to load packages", err)
		os.Exit(1)
	}

	hadErrors := false
	for _, pkg := range pkgs {
		for _, err := range pkg.Errors {
			hadErrors = true
			fmt.Println(err)
		}
	}

	if flagVerbose {
		for _, pkg := range pkgs {
			fmt.Printf("package: %s (%q)\n", pkg.Name, pkg.PkgPath)
		}
	}

	if hadErrors {
		fmt.Println("failed to load all packages specified")
		os.Exit(1)
	}

	driver := drivers.GetDriver("psql")
	dbInfo, err = driver.Assemble(cfg)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "unable to fetch table data:", err)
		os.Exit(1)
	}

	if len(dbInfo.Tables) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "no tables found in database")
		os.Exit(1)
	}

	imports, err := driver.Imports()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to retrieve imports from driver")
		os.Exit(1)
	}

	state := &State{
		DBInfo:  dbInfo,
		Imports: imports,
	}

	calls, warns := findTaggedCalls(pkgs)

	// Change all paths to be relative flagDir
	for i := range calls {
		rel, err := filepath.Rel(flagDir, calls[i].Pos.Filename)
		if err == nil {
			calls[i].Pos.Filename = "./" + rel
		}
	}
	for i := range warns {
		rel, err := filepath.Rel(flagDir, warns[i].Pos.Filename)
		if err == nil {
			warns[i].Pos.Filename = "./" + rel
		}
	}

	for _, w := range warns {
		_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", w)
	}

	errs := checkCalls(state, calls)

	// Prettify output by grouping errors by package as well as
	// finding relative paths for filenames where possible
	//
	// Highly inefficient :D
	printed := make([]bool, len(errs))
	for _, pkg := range pkgs {
		printedPackage := false
		printPkg := func() {
			if printedPackage {
				return
			}
			fmt.Printf("# %s\n", pkg.PkgPath)
			printedPackage = true
		}

		if flagVerbose {
			for _, c := range calls {
				if c.Package != pkg.PkgPath {
					continue
				}

				printPkg()
				filename := c.Pos.Filename
				rel, err := filepath.Rel(flagDir, filename)
				if err == nil {
					filename = "./" + rel
				}
				fmt.Printf("%s:%d:%d check\n", filename, c.Pos.Line, c.Pos.Column)
			}
		}

		for i, err := range errs {
			if printed[i] {
				continue
			}

			switch e := err.(type) {
			case IdentErr:
				if e.Fn.Package == pkg.PkgPath {
					printPkg()
					printed[i] = true
					fmt.Println(e)
				}
			case TypeErr:
				if e.Fn.Package == pkg.PkgPath {
					printPkg()
					printed[i] = true
					fmt.Println(e)
				}
			default:
				printPkg()
				printed[i] = true
				fmt.Println(e)
			}
		}
	}

	if len(errs) != 0 {
		os.Exit(1)
	}
}

func initDriver(driver string) {
	var err error
	driverName := driver
	driverPath := driver

	if strings.ContainsRune(driverName, os.PathSeparator) {
		driverName = strings.Replace(filepath.Base(driverName), "sqlboiler-", "", 1)
		driverName = strings.Replace(driverName, ".exe", "", 1)
	} else {
		driverPath = "sqlboiler-" + driverPath
		if p, err := exec.LookPath(driverPath); err == nil {
			driverPath = p
		}
	}

	driverPath, err = filepath.Abs(driverPath)
	if err != nil {
		panic(errors.Wrap(err, "could not find absolute path to driver"))
	}
	drivers.RegisterBinary(driverName, driverPath)
}

func loadPackages(dir string, pkgNames ...string) ([]*packages.Package, error) {
	pkgCfg := &packages.Config{
		Mode: packages.NeedTypes |
			packages.NeedTypesInfo |
			packages.NeedSyntax |
			packages.NeedFiles |
			packages.NeedName,
		Dir:   dir,
		Tests: false,
	}
	return packages.Load(pkgCfg, pkgNames...)
}

func loadConfig(filename string) (map[string]interface{}, error) {
	mp := make(map[string]interface{})
	_, err := toml.DecodeFile(filename, &mp)
	if err != nil {
		return nil, err
	}

	driverCfgIntf, ok := mp["psql"]
	if !ok {
		return nil, errors.New("no psql key in config file")
	}

	driverCfg, ok := driverCfgIntf.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("psql in config file was wrong type: %T", driverCfgIntf)
	}

	return driverCfg, nil
}
