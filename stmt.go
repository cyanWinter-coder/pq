package pq

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type stmt struct {
	cn   *conn
	name string
	rowsHeader
	colFmtData []byte
	paramTyps  []oid.Oid
	closed     atomic.Bool
}

func (st *stmt) Close() error {
	// Close is idempotent and safe for concurrent use: only the first
	// call sends Close to the server, subsequent calls are no-ops.
	if !st.closed.CompareAndSwap(false, true) {
		return nil
	}
	cn := st.cn
	// The statement is no longer tracked by the connection, no matter
	// what happens below.
	if st.name != "" {
		defer cn.untrackStmt(st.name)
	}

	// If the connection has already been closed, the server-side
	// statement went away with it; there is nothing left to do.
	if cn.isClosed() {
		return nil
	}
	if err := cn.err.get(); err != nil {
		return err
	}

	w := cn.writeBuf(proto.Close)
	w.byte(proto.Sync)
	w.string(st.name)
	err := cn.send(w)
	if err != nil {
		return cn.handleError(err)
	}
	err = cn.send(cn.writeBuf(proto.Sync))
	if err != nil {
		return cn.handleError(err)
	}

	t, _, err := cn.recv1()
	if err != nil {
		return cn.handleError(err)
	}
	if t != proto.CloseComplete {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected close response: %q", t)
	}

	t, r, err := cn.recv1()
	if err != nil {
		return cn.handleError(err)
	}
	if t != proto.ReadyForQuery {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: expected ready for query, but got: %q", t)
	}
	cn.processReadyForQuery(r)

	return nil
}

// Implement [driver.StmtQueryContext].
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	finish := st.cn.watchCancel(ctx, true)
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}

	err := st.exec(args)
	if err != nil {
		finish()
		return nil, st.cn.handleError(err)
	}

	rs := &rows{
		cn:         st.cn,
		rowsHeader: st.rowsHeader,
		finish:     finish,
		portal:     true,
	}
	st.cn.trackRows(rs)
	return rs, nil
}

// Implement [driver.StmtExecContext].
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	defer st.cn.watchCancel(ctx, true)()
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}

	err := st.exec(args)
	if err != nil {
		return nil, st.cn.handleError(err)
	}
	res, _, err := st.cn.readExecuteResponse("simple query")
	return res, st.cn.handleError(err)
}

func (st *stmt) exec(v []driver.NamedValue) error {
	if debugProto {
		fmt.Fprintf(os.Stderr, "         START stmt.exec\n")
		defer fmt.Fprintf(os.Stderr, "         END stmt.exec\n")
	}
	if len(v) >= 65536 {
		return fmt.Errorf("pq: got %d parameters but PostgreSQL only supports 65535 parameters", len(v))
	}
	if len(v) != len(st.paramTyps) {
		return fmt.Errorf("pq: got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	cn := st.cn
	w := cn.writeBuf(proto.Bind)
	w.byte(0) // unnamed portal
	w.string(st.name)

	if cn.cfg.BinaryParameters {
		err := cn.sendBinaryParameters(w, v)
		if err != nil {
			return err
		}
	} else {
		w.int16(0)
		w.int16(len(v))
		for i, x := range v {
			if x.Value == nil {
				w.int32(-1)
			} else {
				b, err := encode(x.Value, st.paramTyps[i])
				if err != nil {
					return err
				}
				if b == nil {
					w.int32(-1)
				} else {
					w.int32(len(b))
					w.bytes(b)
				}
			}
		}
	}
	w.bytes(st.colFmtData)

	w.next(proto.Execute)
	w.byte(0)
	w.int32(0)

	w.next(proto.Sync)
	err := cn.send(w)
	if err != nil {
		return err
	}
	err = cn.readBindResponse()
	if err != nil {
		return err
	}
	return cn.postExecuteWorkaround()
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}
