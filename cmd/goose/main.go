package main

import (
	"database/sql"
	"flag"
	"log"
	"os"

	"github.com/spf13/viper"

	"github.com/pressly/goose"

	// Init DB drivers.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/ziutek/mymysql/godrv"
)

type config struct {
	Driver string
	URL    string
}

var (
	flags           = flag.NewFlagSet("goose", flag.ExitOnError)
	dir             = flags.String("dir", ".", "directory with migration files")
	configPath      = flags.String("config", ".", "directory with configuration file")
	configName      = flags.String("config-name", "dbconf", "name (without extension) for configuration file")
	environmentName = flags.String("env", "default", "environment name")

	configurations map[string]config
)

func main() {
	flags.Usage = usage
	flags.Parse(os.Args[1:])

	args := flags.Args()

	if len(args) > 1 && args[0] == "create" {
		if err := goose.Run("create", nil, *dir, args[1:]...); err != nil {
			log.Fatalf("goose run: %v", err)
		}
		return
	}

	// Panov, read configuration
	var driver, dbstring, command string
	driver, dbstring = loadConfig()
	needParams := 3
	if driver != "" {
		needParams--
	}
	if dbstring != "" {
		needParams--
	}

	if len(args) < needParams {
		flags.Usage()
		return
	}

	if args[0] == "-h" || args[0] == "--help" {
		flags.Usage()
		return
	}

	if driver == "" {
		driver = args[0]
	}
	if dbstring == "" && driver == "" {
		dbstring = args[1]
	} else if dbstring == "" {
		dbstring = args[0]
	}
	command = args[needParams-1]
	log.Printf("Use driver: %s, DSN: ***, command: %s", driver, command)

	switch driver {
	case "postgres", "mysql", "sqlite3", "redshift":
		if err := goose.SetDialect(driver); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("%q driver not supported\n", driver)
	}

	switch dbstring {
	case "":
		log.Fatalf("-dbstring=%q not supported\n", dbstring)
	default:
	}

	if driver == "redshift" {
		driver = "postgres"
	}

	db, err := sql.Open(driver, dbstring)
	if err != nil {
		log.Fatalf("-dbstring=%q: %v\n", dbstring, err)
	}

	arguments := []string{}
	if len(args) > 3 {
		arguments = append(arguments, args[3:]...)
	}

	if err := goose.Run(command, db, *dir, arguments...); err != nil {
		log.Fatalf("goose run: %v", err)
	}
}

func usage() {
	log.Print(usagePrefix)
	flags.PrintDefaults()
	log.Print(usageCommands)
}

var (
	usagePrefix = `Usage: goose [OPTIONS] DRIVER DBSTRING COMMAND

Drivers:
    postgres
    mysql
    sqlite3
    redshift

Examples:
    goose sqlite3 ./foo.db status
    goose sqlite3 ./foo.db create init sql
    goose sqlite3 ./foo.db create add_some_column sql
    goose sqlite3 ./foo.db create fetch_user_data go
    goose sqlite3 ./foo.db up

    goose postgres "user=postgres dbname=postgres sslmode=disable" status
    goose mysql "user:password@/dbname" status
    goose redshift "postgres://user:password@qwerty.us-east-1.redshift.amazonaws.com:5439/db" status

Options:
`

	usageCommands = `
Commands:
    up                   Migrate the DB to the most recent version available
    up-to VERSION        Migrate the DB to a specific VERSION
    down                 Roll back the version by 1
    down-to VERSION      Roll back to a specific VERSION
    redo                 Re-run the latest migration
    reset                Roll back all migrations
    status               Dump the migration status for the current DB
    check                Try to find skipped migrations in given dir 
    version              Print the current version of the database
    create NAME [sql|go] Creates new migration file with next version
`
)

func loadConfig() (driver, dsn string) {
	var err error
	configurations = make(map[string]config)
	viper.SetConfigName(*configName)
	viper.AddConfigPath(*configPath)
	if err = viper.ReadInConfig(); err != nil {
		return
	}

	var (
		defaultSection *viper.Viper
		envSection     *viper.Viper
	)

	defaultSection = viper.Sub("default")
	if envSection = viper.Sub(*environmentName); envSection == nil {
		log.Fatalf("Unable to read section with name %s", *environmentName)
	}

	if driver = envSection.GetString("driver"); driver == "" && defaultSection != nil && *environmentName != "default" {
		driver = defaultSection.GetString("driver")
	}
	dsn = envSection.GetString("url")
	return
}
