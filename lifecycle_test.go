package pq

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"testing"

	"github.com/lib/pq/internal/pqtest"
)

// Statement names are allocated per connection and must be unique, even
// when allocated concurrently.
func TestGnameConcurrentUnique(t *testing.T) {
	t.Parallel()

	cn := &conn{}
	const n = 1000
	names := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			names <- cn.gname()
		}()
	}
	wg.Wait()
	close(names)

	seen := make(map[string]struct{}, n)
	for name := range names {
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate statement name %q", name)
		}
		seen[name] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("got %d unique names, want %d", len(seen), n)
	}
}

// Closing a statement removes it from the connection's bookkeeping, and
// closing it again is a safe no-op.
func TestStmtCloseIdempotent(t *testing.T) {
	t.Parallel()

	c, err := Open(pqtest.DSN(""))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cn := c.(*conn)

	ctx := context.Background()
	st, err := c.(driver.ConnPrepareContext).PrepareContext(ctx, "select 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cn.stmts[st.(*stmt).name]; !ok {
		t.Fatal("prepared statement was not tracked by the connection")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if len(cn.stmts) != 0 {
		t.Fatal("statement was not removed from the connection on Close")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %s", err)
	}

	// The connection must still be usable.
	st2, err := c.(driver.ConnPrepareContext).PrepareContext(ctx, "select 2")
	if err != nil {
		t.Fatal(err)
	}
	if st2.(*stmt).name == st.(*stmt).name {
		t.Fatal("statement name was reused")
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
}

// Closing a result set before it is fully consumed must leave the
// connection usable, both inside and outside a transaction.
func TestRowsEarlyCloseConnReusable(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, db driver.Conn, inTxn bool) {
		ctx := context.Background()
		if inTxn {
			if _, err := db.(driver.ExecerContext).ExecContext(ctx, "begin", nil); err != nil {
				t.Fatal(err)
			}
		}
		st, err := db.(driver.ConnPrepareContext).PrepareContext(ctx,
			"select i from generate_series(1, 1000000) as i")
		if err != nil {
			t.Fatal(err)
		}
		r, err := st.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Read only the first row of a long result set, then close.
		dest := make([]driver.Value, 1)
		if err := r.Next(dest); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}

		// The connection must still be usable.
		st2, err := db.(driver.ConnPrepareContext).PrepareContext(ctx, "select 42")
		if err != nil {
			t.Fatal(err)
		}
		r2, err := st2.(driver.StmtQueryContext).QueryContext(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := r2.Next(dest); err != nil {
			t.Fatal(err)
		}
		if dest[0] != int64(42) {
			t.Fatalf("unexpected value %v", dest[0])
		}
		if err := r2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := st2.Close(); err != nil {
			t.Fatal(err)
		}
		if inTxn {
			if _, err := db.(driver.ExecerContext).ExecContext(ctx, "commit", nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("OutsideTransaction", func(t *testing.T) {
		c, err := Open(pqtest.DSN(""))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		run(t, c, false)
	})
	t.Run("InsideTransaction", func(t *testing.T) {
		c, err := Open(pqtest.DSN(""))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		run(t, c, true)
	})
}

// When reading a result set fails partway, closing it must keep the
// connection usable.
func TestRowsCloseAfterError(t *testing.T) {
	t.Parallel()

	c, err := Open(pqtest.DSN(""))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	st, err := c.(driver.ConnPrepareContext).PrepareContext(ctx,
		"select 1 / (i - 3) from generate_series(1, 10) as i")
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.(driver.StmtQueryContext).QueryContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]driver.Value, 1)
	for {
		err := r.Next(dest)
		if err == nil {
			continue
		}
		if err == io.EOF {
			t.Fatal("expected a query error, got EOF")
		}
		break
	}
	// The error may also be reported by Close; either way the result set
	// must be cleaned up.
	_ = r.Close()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// The connection must still be usable.
	st2, err := c.(driver.ConnPrepareContext).PrepareContext(ctx, "select 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
}

// Simulate a workload where every request prepares statements and closes
// result sets early, concurrently across many connections: statement
// names must not clash and connections must stay usable.
func TestConcurrentPrepareAndEarlyClose(t *testing.T) {
	t.Parallel()

	db := pqtest.MustDB(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				st, err := db.PrepareContext(ctx, "select $1::int, i from generate_series(1, 1000) as i")
				if err != nil {
					t.Error(err)
					return
				}
				r, err := st.QueryContext(ctx, g*1000+i)
				if err != nil {
					t.Error(err)
					return
				}
				// Read a few rows, then close early.
				for j := 0; j < 3 && r.Next(); j++ {
				}
				if err := r.Close(); err != nil {
					t.Error(err)
					return
				}
				if err := st.Close(); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// The pool must still be fully functional.
	var n int
	if err := db.QueryRowContext(ctx, "select 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
}

// Closing a connection cleans up statements and result sets that are
// still open: their Close and Next calls must not touch the dead
// connection.
func TestConnCloseCleansUpStmtsAndRows(t *testing.T) {
	t.Parallel()

	c, err := Open(pqtest.DSN(""))
	if err != nil {
		t.Fatal(err)
	}
	cn := c.(*conn)
	ctx := context.Background()

	st, err := c.(driver.ConnPrepareContext).PrepareContext(ctx,
		"select i from generate_series(1, 1000000) as i")
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.(driver.StmtQueryContext).QueryContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]driver.Value, 1)
	if err := r.Next(dest); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(cn.stmts) != 0 || len(cn.rset) != 0 {
		t.Fatal("connection still tracks statements or result sets after Close")
	}

	// These must be safe no-ops rather than writes on a dead connection.
	if err := st.Close(); err != nil {
		t.Fatalf("stmt.Close after conn.Close: %s", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("rows.Close after conn.Close: %s", err)
	}
	if err := r.Next(dest); err != io.EOF {
		t.Fatalf("rows.Next after conn.Close: got %v, want io.EOF", err)
	}
}
