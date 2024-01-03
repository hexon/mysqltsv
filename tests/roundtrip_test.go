package mysqltsv_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-sql-driver/mysql"
	"github.com/hexon/mysqltsv"
)

var schema = `
CREATE TEMPORARY TABLE roundtrip_test (
	id INT NOT NULL PRIMARY KEY,
	data BLOB NOT NULL
);
`

func TestRoundtrip(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("TEST_DSN")
	if dsn == "" {
		t.Fatalf("Environment variable TEST_DSN is empty")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	var dataRows [][]byte
	for i := 0; 1000 > i; i++ {
		row := make([]byte, rand.Intn(2048))
		for j := range row {
			row[j] = uint8(rand.Intn(255))
		}
		dataRows = append(dataRows, row)
	}

	var buf bytes.Buffer
	e := mysqltsv.NewEncoder(&buf, 2, nil)
	for i, row := range dataRows {
		e.AppendValue(i)
		e.AppendBytes(row)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Encoding failed: %v", err)
	}

	mysql.RegisterReaderHandler("buf", func() io.Reader { return &buf })
	res, err := db.ExecContext(ctx, fmt.Sprintf("LOAD DATA LOCAL INFILE 'Reader::buf' INTO TABLE `roundtrip_test` %s (id, data)", mysqltsv.Escaping))
	if err != nil {
		t.Fatalf("LOAD DATA LOCAL INFILE failed: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}

	rows, err := db.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		t.Fatalf("Failed to SHOW WARNINGS: %v", err)
	}
	for rows.Next() {
		var level, code, message string
		if err := rows.Scan(&level, &code, &message); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Errorf("MySQL warning: %v", message)
	}
	if err := rows.Close(); err != nil {
		t.Errorf("rows.Close: %v", err)
	}
	if n != int64(len(dataRows)) {
		t.Fatalf("Tried to insert %d rows, but succeeded at only %d", len(dataRows), n)
	}

	rows, err = db.QueryContext(ctx, "SELECT data FROM roundtrip_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	for rows.Next() {
		var readData []byte
		if err := rows.Scan(&readData); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		want := dataRows[0]
		if !bytes.Equal(want, readData) {
			t.Errorf("Mismatch!")
			spew.Dump(want)
			spew.Dump(readData)
		}
		dataRows = dataRows[1:]
	}
	if err := rows.Close(); err != nil {
		t.Errorf("rows.Close: %v", err)
	}
}
