package pq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/pqerror"
)

func TestCopyInError(t *testing.T) {
	tests := []struct {
		query   string
		wantErr string
	}{
		{`copy tbl (num) from stdin with binary`, `only text format supported for COPY`},
		{"-- comment\n  /* comment */  copy tbl (num) to stdout", `COPY TO is not supported`},
		{`copy syntax error`, `or:syntax error at or near "error" at column 13|at or near "error": syntax error`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			tx := pqtest.Begin(t, pqtest.MustDB(t))
			pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

			_, err := tx.Prepare(tt.query)
			if !pqtest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error:\nhave: %s\nwant: %s", err, tt.wantErr)
			}
			// Check that the protocol is in a valid state
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCopyInErrorWrongType(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num) from stdin`, db)
	stmt.MustExec(t, "Héllö\n ☃!\r\t\\")
	_, err := stmt.Exec()
	mustAs(t, err, pqerror.InvalidTextRepresentation)
}

func TestCopyInErrorOutsideTransaction(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	_, err := db.Prepare(`copy tbl (num) from stdin`)
	if err != errCopyNotSupportedOutsideTxn {
		t.Errorf("wrong error: %v", err)
	}
}

func TestCopyInNull(t *testing.T) {
	tests := []struct {
		null any
		copy string
	}{
		{nil, `copy tbl (i, t) from stdin`},
		{`NULL`, `copy tbl (i, t) from stdin with null 'NULL'`},
		{``, `copy tbl (i, t) from stdin with null ''`},
		{`\N`, `copy tbl (i, t) from stdin with null '\\N'`},

		// The default doesn't work as copyin.Exec() calls appendEncodedText(),
		// which escapes \N to \\N. To fix it we need to read query, see if
		// "WITH NULL" was passed, and don't escape that text (of the default of
		// \N).
		//{`\N`, `copy tbl (i, t) from stdin`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db := pqtest.MustDB(t)
			tx := pqtest.Begin(t, db)

			pqtest.Exec(t, tx, `create temp table tbl (i int, t text)`)
			stmt := pqtest.Prepare(t, tx, tt.copy, db)
			stmt.MustExec(t, 42, "forty-two")
			stmt.MustExec(t, tt.null, tt.null)
			stmt.MustExec(t)
			stmt.MustClose(t)

			rows := pqtest.Query[any](t, tx, `select * from tbl`)
			want := []map[string]any{
				{"i": int64(42), "t": "forty-two"},
				{"i": nil, "t": nil},
			}
			if !reflect.DeepEqual(rows, want) {
				t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
			}
		})
	}
}

func TestCopyInMultipleValues(t *testing.T) {
	tests := []struct {
		query string
	}{
		{`copy tbl (a, b) from stdin`},
		{`copy tbl from stdin`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			db := pqtest.MustDB(t)
			tx := pqtest.Begin(t, db)
			pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)

			stmt := pqtest.Prepare(t, tx, tt.query, db)
			for i := range 500 {
				stmt.MustExec(t, int64(i), strings.Repeat("#", 500))
			}

			res := stmt.MustExec(t)
			rows, err := res.RowsAffected()
			if err != nil || rows != 500 {
				t.Fatalf("\nerr: %v\nrows: %v", err, rows)
			}

			n, err := res.LastInsertId()
			if n != 0 || err == nil || err.Error() != `LastInsertId is not supported by this driver` {
				t.Errorf("n=%d; err=%v", n, err)
			}

			stmt.MustClose(t)

			num := pqtest.Query[int](t, tx, `select count(*) from tbl`)[0]["count"]
			if num != 500 {
				t.Fatalf("expected 500 items, not %d", num)
			}
		})
	}
}

func TestCopyInRaiseStmtTrigger(t *testing.T) {
	pqtest.SkipCockroach(t) // "unimplemented: cannot create user-defined functions under a temporary schema"
	t.Parallel()
	db := pqtest.MustDB(t)
	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl (a int, b varchar)`)
	pqtest.Exec(t, tx, `
		create or replace function pg_temp.temptest()
		returns trigger as
		$BODY$ begin
			raise notice 'Hello world';
			return new;
		end $BODY$
		language plpgsql
	`)
	pqtest.Exec(t, tx, `
		create trigger temptest_trigger
		before insert on tbl
		for each row execute procedure pg_temp.temptest()
	`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (a, b) from stdin`, db)
	stmt.MustExec(t, int64(1), strings.Repeat("#", 500))
	stmt.MustExec(t)
	stmt.MustClose(t)

	rows := pqtest.Query[any](t, tx, `select * from tbl`)
	want := []map[string]any{{
		"a": int64(1),
		"b": strings.Repeat("#", 500),
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
	}
}

func TestCopyInTypes(t *testing.T) {
	pqtest.SkipCockroach(t) // https://github.com/cockroachdb/cockroach/issues/167309
	t.Parallel()
	db := pqtest.MustDB(t)
	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl (num integer, text varchar, blob bytea, nothing varchar)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num, text, blob, nothing) from stdin`, db)
	stmt.MustExec(t, int64(1234567890), "Héllö\n ☃!\r\t\\", []byte{0, 255, 9, 10, 13}, nil)
	stmt.MustExec(t)
	stmt.MustClose(t)

	rows := pqtest.Query[any](t, tx, `select * from tbl`)
	want := []map[string]any{{
		"num":     int64(1234567890),
		"text":    "Héllö\n ☃!\r\t\\",
		"blob":    []byte{0, 255, 9, 10, 13},
		"nothing": nil,
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("\nhave: %#v\nwant: %#v", rows, want)
	}
}

// Tests for connection errors in copyin.resploop()
func TestCopyInRespLoopConnectionError(t *testing.T) {
	pqtest.SkipCockroach(t) // Doesn't implement pg_terminate_backend()

	// Executes f in a backoff loop until it doesn't return an error. If this
	// doesn't happen within duration, t.Fatal is called with the latest error.
	retry := func(t *testing.T, duration time.Duration, f func() error) {
		start := time.Now()
		next := time.Millisecond * 100
		for {
			err := f()
			if err == nil {
				return
			}
			if time.Since(start) > duration {
				t.Fatal(err)
			}
			time.Sleep(next)
			next *= 2
		}
	}

	t.Parallel()
	db := pqtest.MustDB(t)
	tx := pqtest.Begin(t, db)

	pid := pqtest.Query[int64](t, tx, `select pg_backend_pid() as pid`)
	pqtest.Exec(t, tx, "create temp table tbl (a int)")
	stmt := pqtest.Prepare(t, tx, `copy tbl (a) from stdin`, db)
	pqtest.Exec(t, db, `select pg_terminate_backend($1)`, pid[0]["pid"])

	var err error
	retry(t, time.Second*5, func() error {
		_, err = stmt.Exec()
		if err == nil {
			return fmt.Errorf("expected error")
		}
		return nil
	})
	switch pge := err.(type) {
	case *Error:
		if pge.Code.Name() != "admin_shutdown" {
			t.Fatalf("expected admin_shutdown, got %s", pge.Code.Name())
		}
	case *net.OpError:
		// ignore
	default:
		if err == driver.ErrBadConn {
			// likely an EPIPE
		} else if err == errCopyInClosed {
			// ignore
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestCopyLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		where string
		line  int64
		ok    bool
	}{
		{`COPY tbl, line 42, column c: "notanint"`, 42, true},
		{`COPY tbl, line 7`, 7, true},
		{`COPY "weird, name", line 3, column x`, 3, true},
		{`SQL statement "insert into tbl values (1)"`, 0, false},
		{`COPY tbl`, 0, false},
		{"", 0, false},
	}
	for _, tt := range tests {
		e := &Error{Where: tt.where}
		line, ok := e.CopyLine()
		if line != tt.line || ok != tt.ok {
			t.Errorf("CopyLine(%q) = %d, %t; want %d, %t", tt.where, line, ok, tt.line, tt.ok)
		}
		if _, _, ok := e.CopyProgress(); ok {
			t.Errorf("CopyProgress(%q) ok = true; want false for non-COPY error", tt.where)
		}
	}
}

// The server rejects a row mid-COPY: the returned *Error carries the
// failing line number and the batch ledger, and the connection is back in a
// reusable state afterwards.
func TestCopyInErrorLineAndProgress(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	db.SetMaxOpenConns(1) // the checks below reuse the very same connection
	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl (num integer, txt text)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num, txt) from stdin`, db)
	// Rows larger than the flush threshold are transmitted immediately, each
	// as its own batch.
	stmt.MustExec(t, int64(1), strings.Repeat("a", 70*1024))
	stmt.MustExec(t, int64(2), strings.Repeat("b", 70*1024))
	stmt.MustExec(t, "notanint", "x") // stays in the buffer until the sync

	_, err := stmt.Exec()
	pqErr := mustAs(t, err, pqerror.InvalidTextRepresentation)

	if line, ok := pqErr.CopyLine(); !ok || line != 3 {
		t.Errorf("CopyLine() = %d, %t; want 3, true (Where: %q)", line, ok, pqErr.Where)
	}
	rowsSent, batchesSent, ok := pqErr.CopyProgress()
	if !ok {
		t.Fatal("CopyProgress() ok = false; want true")
	}
	if rowsSent != 3 || batchesSent != 3 {
		t.Errorf("CopyProgress() = %d rows, %d batches; want 3, 3", rowsSent, batchesSent)
	}

	// The connection must be in a clean state: roll back and reuse it.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	pqtest.Query[int64](t, db, `select 1 as n`)
}

// Canceling the caller's context stops the COPY at the next batch boundary,
// terminates it with CopyFail, and leaves the connection reusable.
func TestCopyInCancel(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)
	db.SetMaxOpenConns(1) // the checks below reuse the very same connection
	tx := pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl (num integer)`)

	stmt := pqtest.Prepare(t, tx, `copy tbl (num) from stdin`, db)
	stmt.MustExec(t, int64(1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := stmt.ExecContext(ctx, int64(2))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	// The COPY is aborted, so the transaction is dead, but the connection
	// must be back in a clean state: roll back and reuse it.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	pqtest.Query[int64](t, db, `select 1 as n`)

	// A new COPY on the same connection works.
	tx = pqtest.Begin(t, db)
	pqtest.Exec(t, tx, `create temp table tbl2 (num integer)`)
	stmt = pqtest.Prepare(t, tx, `copy tbl2 (num) from stdin`, db)
	stmt.MustExec(t, int64(1))
	stmt.MustExec(t)
	stmt.MustClose(t)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// CopyData with a canceled context terminates the COPY and leaves the
// connection reusable.
func TestCopyDataCancel(t *testing.T) {
	t.Parallel()
	db := pqtest.MustDB(t)

	dc, err := db.Driver().Open(pqtest.DSN(""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dc.Close() })
	cn := dc.(interface {
		driver.ExecerContext
		driver.ConnBeginTx
		driver.ConnPrepareContext
	})
	ctx := context.Background()
	if _, err := cn.ExecContext(ctx, `create temp table tbl (num integer)`, nil); err != nil {
		t.Fatal(err)
	}
	tx, err := cn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := cn.PrepareContext(ctx, `copy tbl (num) from stdin`)
	if err != nil {
		t.Fatal(err)
	}
	ci := st.(interface {
		CopyData(context.Context, string) (driver.Result, error)
	})

	if _, err := ci.CopyData(ctx, "1"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ci.CopyData(canceled, "2"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// The connection is reusable.
	if _, err := cn.ExecContext(ctx, `select 1`, nil); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCopyIn(b *testing.B) {
	db := pqtest.MustDB(b)
	tx := pqtest.Begin(b, db)

	pqtest.Exec(b, tx, `create temp table tbl (a int, b varchar)`)
	stmt := pqtest.Prepare(b, tx, `copy tbl (a, b) from stdin`, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.MustExec(b, int64(i), "hello world!")
	}

	stmt.MustExec(b)
	stmt.MustClose(b)

	rows := pqtest.Query[int](b, tx, `select count(*) from tbl`)
	if rows[0]["count"] != b.N {
		b.Fatalf("expected %d items, not %d", b.N, rows[0]["count"])
	}
}
