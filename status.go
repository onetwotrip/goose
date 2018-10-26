package goose

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

// Status prints the status of all migrations.
func Status(db *sql.DB, dir string) error {
	// collect all migrations
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db); err != nil {
		return err
	}

	log.Println("    Applied At                  Migration")
	log.Println("    =======================================")
	for _, migration := range migrations {
		printMigrationStatus(db, migration.Version, filepath.Base(migration.Source))
	}

	return nil
}

func printMigrationStatus(db *sql.DB, version int64, script string) {
	var row MigrationRecord
	q := fmt.Sprintf("SELECT tstamp, is_applied FROM goose_db_version WHERE version_id=%d ORDER BY tstamp DESC LIMIT 1", version)
	e := db.QueryRow(q).Scan(&row.TStamp, &row.IsApplied)

	if e != nil && e != sql.ErrNoRows {
		log.Fatal(e)
	}

	var appliedAt string

	if row.IsApplied {
		appliedAt = row.TStamp.Format(time.ANSIC)
	} else {
		appliedAt = "Pending"
	}

	log.Printf("    %-24s -- %v\n", appliedAt, script)
}

// Check checks there is no skipped migrations.
func Check(db *sql.DB, dir string) error {
	// collect all migrations
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db); err != nil {
		return err
	}

	query := fmt.Sprintf(`
		SELECT goose_db_version.version_id
		FROM goose_db_version
		  LEFT JOIN goose_db_version AS newer ON newer.version_id = goose_db_version.version_id AND newer.tstamp > goose_db_version.tstamp
		WHERE goose_db_version.version_id > %d
		  AND newer.id IS NULL
		  AND goose_db_version.is_applied
		ORDER BY goose_db_version.version_id 
	`, minVersion)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var skipped []int64
	current := minVersion
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return err
		}

		migration, err := migrations.Next(current)
		for err == nil && migration.Version < version {
			skipped = append(skipped, migration.Version)
			current = migration.Version
			migration, err = migrations.Next(current)
		}
		switch err {
		case nil:
			if migration.Version == version {
				current = migration.Version
			}
		case ErrNoNextVersion:
		default:
			return err
		}
	}

	if len(skipped) > 0 {
		return fmt.Errorf("migrations %v has been skipped", skipped)
	}

	return nil
}
