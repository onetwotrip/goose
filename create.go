package goose

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"text/template"
	"time"
)

const versionFormatDate = "20060102150405"

// Create writes a new blank migration file.
func Create(db *sql.DB, dir, name, migrationType string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	// Initial version.
	version := "00001"

	if last, err := migrations.Last(); err == nil {
		version = nextVersion(last.Version)
	}

	filename := fmt.Sprintf("%v_%v.%v", version, name, migrationType)

	fpath := filepath.Join(dir, filename)
	tmpl := sqlMigrationTemplate
	if migrationType == "go" {
		tmpl = goSQLMigrationTemplate
	}

	path, err := writeTemplateToFile(fpath, tmpl, version)
	if err != nil {
		return err
	}

	log.Printf("Created new file: %s\n", path)
	return nil
}

func nextVersion(last int64) string {
	if _, err := time.Parse(versionFormatDate, strconv.FormatInt(last, 10)); err == nil {
		return time.Now().Format(versionFormatDate)
	}

	return fmt.Sprintf("%05v", last+1)
}

func writeTemplateToFile(path string, t *template.Template, version string) (string, error) {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to create file: %v already exists", path)
	}

	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	err = t.Execute(f, version)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

var sqlMigrationTemplate = template.Must(template.New("goose.sql-migration").Parse(`-- +goose Up
-- SQL in this section is executed when the migration is applied.

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
`))

var goSQLMigrationTemplate = template.Must(template.New("goose.go-migration").Parse(`package migration

import (
	"database/sql"
	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up{{.}}, Down{{.}})
}

func Up{{.}}(tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	return nil
}

func Down{{.}}(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
`))
