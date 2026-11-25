package pq

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq/internal/proto"
)

var (
	errCopyInClosed               = errors.New("pq: copyin statement has already been closed")
	errBinaryCopyNotSupported     = errors.New("pq: only text format supported for COPY")
	errCopyToNotSupported         = errors.New("pq: COPY TO is not supported")
	errCopyNotSupportedOutsideTxn = errors.New("pq: COPY is only allowed inside a transaction")
	errCopyTxnEnd                 = errors.New("pq: COPY aborted because the transaction ended")
)

type copyin struct {
	cn      *conn
	buffer  []byte
	rowData chan []byte
	done    chan bool
	closed  bool

	// Batch ledger. Every flush transmits exactly one CopyData message; the
	// message's length prefix is the batch boundary on the wire. The server
	// confirms the stream cumulatively (the row count in CommandComplete)
	// and reports a failing batch immediately with an ErrorResponse, so the
	// ledger identifies exactly which rows had reached the server when a
	// COPY fails. See Error.CopyLine and Error.CopyProgress.
	rowsBuffered int64 // rows in buffer, not yet transmitted (writer only)
	rowsSent     atomic.Int64
	batchesSent  atomic.Int64

	mu struct {
		sync.Mutex
		err error
		driver.Result
	}
}

const (
	ciBufferSize = 64 * 1024
	// flush buffer before the buffer is filled up and needs reallocation
	ciBufferFlushSize = 63 * 1024
)

func (cn *conn) prepareCopyIn(q string) (_ driver.Stmt, resErr error) {
	if !cn.isInTransaction() {
		return nil, errCopyNotSupportedOutsideTxn
	}

	ci := &copyin{
		cn:      cn,
		buffer:  make([]byte, 0, ciBufferSize),
		rowData: make(chan []byte),
		done:    make(chan bool, 1),
	}
	// add CopyData identifier + 4 bytes for message length
	ci.buffer = append(ci.buffer, byte(proto.CopyDataRequest), 0, 0, 0, 0)

	b := cn.writeBuf(proto.Query)
	b.string(q)
	err := cn.send(b)
	if err != nil {
		return nil, err
	}

awaitCopyInResponse:
	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, err
		}
		switch t {
		case proto.CopyInResponse:
			if r.byte() != 0 {
				resErr = errBinaryCopyNotSupported
				break awaitCopyInResponse
			}
			cn.copyIn.Store(ci)
			go ci.resploop()
			return ci, nil
		case proto.CopyOutResponse:
			resErr = errCopyToNotSupported
			break awaitCopyInResponse
		case proto.ErrorResponse:
			resErr = parseError(r, q)
		case proto.ReadyForQuery:
			if resErr == nil {
				ci.setBad(driver.ErrBadConn)
				return nil, fmt.Errorf("pq: unexpected ReadyForQuery in response to COPY")
			}
			cn.processReadyForQuery(r)
			return nil, resErr
		default:
			ci.setBad(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unknown response for copy query: %q", t)
		}
	}

	// something went wrong, abort COPY before we return
	b = cn.writeBuf(proto.CopyFail)
	b.string(resErr.Error())
	err = cn.send(b)
	if err != nil {
		return nil, err
	}

	for {
		t, r, err := cn.recv1()
		if err != nil {
			return nil, err
		}

		switch t {
		case proto.CopyDoneResponse, proto.CommandComplete, proto.ErrorResponse:
		case proto.ReadyForQuery:
			// correctly aborted, we're done
			cn.processReadyForQuery(r)
			return nil, resErr
		default:
			ci.setBad(driver.ErrBadConn)
			return nil, fmt.Errorf("pq: unknown response for CopyFail: %q", t)
		}
	}
}

func (ci *copyin) flush(buf []byte) (int, error) {
	if len(buf)-1 > proto.MaxUint32 {
		return 0, errors.New("pq: too many columns")
	}
	if debugProto {
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", proto.RequestCode(buf[0]), len(buf)-5, buf[5:])
	}
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1)) // Set message length (without message identifier).
	return ci.cn.c.Write(buf)
}

// flushBatch transmits the buffered rows as a single CopyData message — one
// batch with an explicit length-delimited boundary on the wire — and records
// it in the batch ledger.
//
// If the write fails before a single byte was sent, the message framing is
// still intact and terminate() is used to return the connection to a
// reusable state. A partially written message desynchronizes the protocol
// stream; such a connection is beyond repair and is marked bad.
func (ci *copyin) flushBatch() error {
	n, err := ci.flush(ci.buffer)
	if err != nil {
		if n > 0 {
			return ci.cn.handleError(err)
		}
		return ci.terminate(err)
	}
	ci.batchesSent.Add(1)
	ci.rowsSent.Add(ci.rowsBuffered)
	ci.rowsBuffered = 0
	// reset buffer, keep bytes for message identifier and length
	ci.buffer = ci.buffer[:5]
	return nil
}

// sendCopyFail transmits a CopyFail message. It does not use the scratch
// buffer, which resploop could be using.
func (ci *copyin) sendCopyFail(reason string) error {
	if debugProto {
		fmt.Fprintf(os.Stderr, "CLIENT → %-20s %5d  %q\n", proto.CopyFail, len(reason)+1, reason)
	}
	buf := make([]byte, 0, len(reason)+6)
	buf = append(buf, byte(proto.CopyFail), 0, 0, 0, 0)
	buf = append(buf, reason...)
	buf = append(buf, 0)
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))
	_, err := ci.cn.c.Write(buf)
	return err
}

// terminate aborts the COPY after a local failure — a write error or caller
// cancellation — and returns reason to the caller.
//
// It sends CopyFail so the server stops waiting for more data, discards any
// rows still in the client buffer, and waits for resploop to drain the
// server's response up to ReadyForQuery. When this succeeds the connection
// is back in a clean, reusable state (the transaction itself is aborted
// server-side and still needs to be rolled back by the caller) instead of
// being discarded by the connection pool. If the connection is already
// dead, or dies while terminating, it is marked bad as before.
//
// terminate is idempotent: a COPY that was already terminated or synced must
// not see a second CopyFail, which the server would reject as an unexpected
// message.
func (ci *copyin) terminate(reason error) error {
	if ci.closed {
		return reason
	}
	ci.closed = true
	// Drain the remaining buffer: these rows were never transmitted, so
	// they are discarded rather than sent.
	ci.buffer = ci.buffer[:5]
	ci.rowsBuffered = 0

	if err := ci.getBad(); err != nil {
		// The connection is already dead and resploop has exited; there is
		// nothing left to resynchronize.
		return reason
	}
	if err := ci.sendCopyFail("pq: " + reason.Error()); err != nil {
		ci.cn.handleError(err)
		return reason
	}
	// resploop receives the ErrorResponse caused by CopyFail and then
	// ReadyForQuery, and signals done.
	<-ci.done
	return reason
}

// interruptOnCancel returns a stop function. While active, cancellation of
// ctx expires the connection's write deadline so that a blocked flush
// returns immediately instead of pinning the connection until the TCP
// timeout. The stop function waits for the watcher to exit and clears the
// deadline again; it must be called before the connection is reused, and is
// idempotent.
func (ci *copyin) interruptOnCancel(ctx context.Context) (stop func()) {
	done := ctx.Done()
	if done == nil { // "may return nil if this context can never be canceled"
		return func() {}
	}
	finish := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-done:
			ci.cn.c.SetWriteDeadline(time.Now())
		case <-finish:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(finish)
			<-exited
			ci.cn.c.SetWriteDeadline(time.Time{})
		})
	}
}

func (ci *copyin) resploop() {
	// The COPY is fully synced with the server once resploop exits.
	defer ci.cn.copyIn.CompareAndSwap(ci, nil)
	for {
		var r readBuf
		t, err := ci.cn.recvMessage(&r)
		if err != nil {
			ci.setBad(driver.ErrBadConn)
			ci.setError(err)
			ci.done <- true
			return
		}
		switch t {
		case proto.CommandComplete:
			// complete
			res, _, err := ci.cn.parseComplete(r.string())
			if err != nil {
				panic(err)
			}
			ci.setResult(res)
		case proto.NoticeResponse:
			if n := ci.cn.noticeHandler; n != nil {
				n(parseError(&r, ""))
			}
		case proto.ReadyForQuery:
			ci.cn.processReadyForQuery(&r)
			ci.done <- true
			return
		case proto.ErrorResponse:
			err := parseError(&r, "")
			// Attach the batch ledger so the caller can tell exactly which
			// rows had reached the server when it rejected the stream.
			err.copyRowsSent = ci.rowsSent.Load()
			err.copyBatchesSent = ci.batchesSent.Load()
			err.copyIn = true
			ci.setError(err)
		default:
			ci.setBad(driver.ErrBadConn)
			ci.setError(fmt.Errorf("unknown response during CopyIn: %q", t))
			ci.done <- true
			return
		}
	}
}

func (ci *copyin) setBad(err error) {
	ci.cn.err.set(err)
}

func (ci *copyin) getBad() error {
	return ci.cn.err.get()
}

func (ci *copyin) err() error {
	ci.mu.Lock()
	err := ci.mu.err
	ci.mu.Unlock()
	return err
}

// setError() sets ci.err if one has not been set already.  Caller must not be
// holding ci.Mutex.
func (ci *copyin) setError(err error) {
	ci.mu.Lock()
	if ci.mu.err == nil {
		ci.mu.err = err
	}
	ci.mu.Unlock()
}

func (ci *copyin) setResult(result driver.Result) {
	ci.mu.Lock()
	ci.mu.Result = result
	ci.mu.Unlock()
}

func (ci *copyin) getResult() driver.Result {
	ci.mu.Lock()
	result := ci.mu.Result
	ci.mu.Unlock()
	if result == nil {
		return driver.RowsAffected(0)
	}
	return result
}

func (ci *copyin) NumInput() int {
	return -1
}

func (ci *copyin) Query(v []driver.Value) (r driver.Rows, err error) {
	return nil, ErrNotSupported
}

// Exec inserts values into the COPY stream. The insert is asynchronous
// and Exec can return errors from previous Exec calls to the same
// COPY stmt.
//
// You need to call Exec(nil) to sync the COPY stream and to get any
// errors from pending data, since Stmt.Close() doesn't return errors
// to the user.
func (ci *copyin) Exec(v []driver.Value) (driver.Result, error) {
	return ci.execContext(context.Background(), v)
}

// ExecContext implements driver.StmtExecContext. When ctx is canceled the
// COPY is terminated at the next batch boundary: no further rows are
// written, buffered rows are discarded, and the connection is returned to a
// reusable state (see terminate).
func (ci *copyin) ExecContext(ctx context.Context, v []driver.Value) (driver.Result, error) {
	return ci.execContext(ctx, v)
}

func (ci *copyin) execContext(ctx context.Context, v []driver.Value) (driver.Result, error) {
	if ci.closed {
		return nil, errCopyInClosed
	}
	if err := ci.getBad(); err != nil {
		return nil, err
	}
	if err := ci.err(); err != nil {
		return nil, err
	}

	if len(v) == 0 {
		if err := ctx.Err(); err != nil {
			return driver.RowsAffected(0), ci.terminate(err)
		}
		if err := ci.Close(); err != nil {
			return driver.RowsAffected(0), err
		}
		return ci.getResult(), nil
	}

	if err := ctx.Err(); err != nil {
		return nil, ci.terminate(err)
	}
	stop := ci.interruptOnCancel(ctx)
	defer stop()

	var (
		numValues = len(v)
		err       error
	)
	for i, value := range v {
		ci.buffer, err = appendEncodedText(ci.buffer, value)
		if err != nil {
			return nil, ci.cn.handleError(err)
		}
		if i < numValues-1 {
			ci.buffer = append(ci.buffer, '\t')
		}
	}

	ci.buffer = append(ci.buffer, '\n')
	ci.rowsBuffered++

	if len(ci.buffer) > ciBufferFlushSize {
		// A cancellation takes effect at the batch boundary: stop writing
		// and terminate the COPY instead of transmitting another batch.
		if err := ctx.Err(); err != nil {
			stop()
			return nil, ci.terminate(err)
		}
		if err := ci.flushBatch(); err != nil {
			stop()
			return nil, err
		}
	}

	return driver.RowsAffected(0), nil
}

// CopyData inserts a raw string into the COPY stream. The insert is
// asynchronous and CopyData can return errors from previous CopyData calls to
// the same COPY stmt.
//
// When ctx is canceled, CopyData stops writing at the next batch boundary,
// discards any rows still in the buffer, terminates the COPY with CopyFail,
// and returns ctx.Err(). The connection is left in a clean, reusable state.
//
// You need to call Exec(nil) to sync the COPY stream and to get any
// errors from pending data, since Stmt.Close() doesn't return errors
// to the user.
func (ci *copyin) CopyData(ctx context.Context, line string) (driver.Result, error) {
	if ci.closed {
		return nil, errCopyInClosed
	}
	if err := ci.getBad(); err != nil {
		return nil, err
	}
	if err := ci.err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, ci.terminate(err)
	}
	stop := ci.interruptOnCancel(ctx)
	defer stop()

	ci.buffer = append(ci.buffer, []byte(line)...)
	ci.buffer = append(ci.buffer, '\n')
	ci.rowsBuffered += int64(strings.Count(line, "\n") + 1)

	if len(ci.buffer) > ciBufferFlushSize {
		// A cancellation takes effect at the batch boundary: stop writing
		// and terminate the COPY instead of transmitting another batch.
		if err := ctx.Err(); err != nil {
			stop()
			return nil, ci.terminate(err)
		}
		if err := ci.flushBatch(); err != nil {
			stop()
			return nil, err
		}
	}

	return driver.RowsAffected(0), nil
}

func (ci *copyin) Close() error {
	if ci.closed { // Don't do anything, we're already closed
		return nil
	}
	ci.closed = true

	if err := ci.getBad(); err != nil {
		return err
	}

	if len(ci.buffer) > 5 { // more than the message identifier and length
		// flushBatch terminates the COPY itself when the flush fails, so
		// there is nothing left to sync here.
		if err := ci.flushBatch(); err != nil {
			return err
		}
	}
	// Avoid touching the scratch buffer as resploop could be using it.
	err := ci.cn.sendSimpleMessage(proto.CopyDoneRequest)
	if err != nil {
		return ci.cn.handleError(err)
	}

	<-ci.done

	if err := ci.err(); err != nil {
		return err
	}
	return nil
}
