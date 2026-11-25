package pq

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type noRows struct{}

var emptyRows noRows

var _ driver.Result = noRows{}

func (noRows) LastInsertId() (int64, error) { return 0, errNoLastInsertID }
func (noRows) RowsAffected() (int64, error) { return 0, errNoRowsAffected }

type (
	rowsHeader struct {
		colNames []string
		colTyps  []fieldDesc
		colFmts  []format
	}
	rows struct {
		cn     *conn
		finish func()
		rowsHeader
		done   bool
		rb     readBuf
		result driver.Result
		tag    string

		// portal is true when the rows were produced by an
		// extended-protocol Execute, meaning the server may still hold a
		// portal for the unread rows which should be closed explicitly.
		portal bool

		next *rowsHeader
	}
)

func (rs *rows) Close() error {
	if rs.finish != nil {
		defer rs.finish()
	}
	cn := rs.cn
	if cn == nil {
		return nil
	}
	// The result set is no longer tracked by the connection, no matter
	// what happens below.
	defer cn.untrackRows(rs)

	if rs.done {
		return nil
	}

	// If the connection is broken or has been closed there is no way —
	// and no need — to tidy up: it will be discarded rather than reused.
	if cn.err.get() != nil || cn.isClosed() {
		rs.done = true
		return nil
	}

	if rs.portal {
		// The result set is abandoned before being fully consumed. Ask
		// the server to close the portal so it can release the resources
		// held by the unread rows, then discard messages until the
		// connection is back in sync and can be reused.
		return rs.closePortal()
	}

	// Simple-protocol result sets have no portal to close; discard the
	// remaining messages so the connection can be reused. Keep draining
	// even when an error occurs partway, otherwise the unread messages
	// would desynchronize the connection and stall the next query.
	var resErr error
	for !rs.done {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			// rs.Next can return io.EOF on both ReadyForQuery and
			// RowDescription (used with HasNextResultSet). We need to fetch
			// messages until we hit a ReadyForQuery, which is done by waiting
			// for done to be set.
		default:
			if resErr == nil {
				resErr = err
			}
			if cn.err.get() != nil {
				// The connection is broken; draining further could
				// block forever. It will be discarded anyway.
				rs.done = true
			}
		}
	}
	return resErr
}

// closePortal sends Close for the unnamed portal followed by Sync, and
// then discards messages until the connection is synchronized again.
// The connection is left usable for the next query.
func (rs *rows) closePortal() error {
	cn := rs.cn

	w := cn.writeBuf(proto.Close)
	w.byte(proto.RequestCode('P')) // close a portal
	w.string("")                   // the unnamed portal
	w.next(proto.Sync)
	if err := cn.send(w); err != nil {
		rs.done = true
		return cn.handleError(err)
	}

	// Two ReadyForQuery messages are now outstanding: one for the Sync
	// sent along with Execute, and one for the Sync sent above. Discard
	// messages until both have been received.
	var resErr error
	for n := 0; n < 2; {
		t, err := cn.recv1Buf(&rs.rb)
		if err != nil {
			rs.done = true
			return cn.handleError(err)
		}
		switch t {
		case proto.ReadyForQuery:
			cn.processReadyForQuery(&rs.rb)
			n++
		case proto.ErrorResponse:
			// This is either the deferred result of the Execute, or
			// the Close failing because the portal no longer existed
			// (e.g. its transaction had already ended). Either way the
			// connection itself is still usable; report the first
			// error to the caller.
			if resErr == nil {
				resErr = parseError(&rs.rb, "")
			}
		case proto.DataRow, proto.CommandComplete, proto.EmptyQueryResponse,
			proto.CloseComplete, proto.RowDescription:
			// Discard; the result set is being abandoned.
		default:
			cn.err.set(driver.ErrBadConn)
			rs.done = true
			return fmt.Errorf("pq: unexpected message while closing result set: %q", t)
		}
	}
	rs.done = true
	return cn.handleError(resErr)
}

func (rs *rows) Columns() []string {
	return rs.colNames
}

func (rs *rows) Result() driver.Result {
	if rs.result == nil {
		return emptyRows
	}
	return rs.result
}

func (rs *rows) Tag() string {
	return rs.tag
}

func (rs *rows) Next(dest []driver.Value) (resErr error) {
	if rs.done {
		return io.EOF
	}
	if err := rs.cn.err.getForNext(); err != nil {
		return err
	}

	for {
		t, err := rs.cn.recv1Buf(&rs.rb)
		if err != nil {
			return rs.cn.handleError(err)
		}
		switch t {
		case proto.ErrorResponse:
			resErr = parseError(&rs.rb, "")
		case proto.CommandComplete, proto.EmptyQueryResponse:
			if t == proto.CommandComplete {
				rs.result, rs.tag, err = rs.cn.parseComplete(rs.rb.string())
				if err != nil {
					return rs.cn.handleError(err)
				}
			}
			continue
		case proto.ReadyForQuery:
			rs.cn.processReadyForQuery(&rs.rb)
			rs.done = true
			if resErr != nil {
				return rs.cn.handleError(resErr)
			}
			return io.EOF
		case proto.DataRow:
			n := rs.rb.int16()
			if resErr != nil {
				rs.cn.err.set(driver.ErrBadConn)
				return fmt.Errorf("pq: unexpected DataRow after error %s", resErr)
			}
			if n < len(dest) {
				dest = dest[:n]
			}
			for i := range dest {
				l := rs.rb.int32()
				if l == -1 {
					dest[i] = nil
					continue
				}
				dest[i], err = decode(&rs.cn.parameterStatus, rs.rb.next(l), rs.colTyps[i].OID, rs.colFmts[i])
				if err != nil {
					return rs.cn.handleError(err)
				}
			}
			return rs.cn.handleError(resErr)
		case proto.RowDescription:
			next := parsePortalRowDescribe(&rs.rb)
			rs.next = &next
			return io.EOF
		default:
			return fmt.Errorf("pq: unexpected message after execute: %q", t)
		}
	}
}

func (rs *rows) HasNextResultSet() bool {
	hasNext := rs.next != nil && !rs.done
	return hasNext
}

func (rs *rows) NextResultSet() error {
	if rs.next == nil {
		return io.EOF
	}
	rs.rowsHeader = *rs.next
	rs.next = nil
	return nil
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (rs *rows) ColumnTypeScanType(index int) reflect.Type {
	return rs.colTyps[index].Type()
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (rs *rows) ColumnTypeDatabaseTypeName(index int) string {
	if rs.cn.parameterStatus.isRedshift {
		if n, ok := redshiftTypeName[rs.colTyps[index].OID]; ok {
			return n
		}
	}
	return rs.colTyps[index].Name()
}

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (rs *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return rs.colTyps[index].Length()
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return rs.colTyps[index].PrecisionScale()
}

const headerSize = 4

type fieldDesc struct {
	// The object ID of the data type.
	OID oid.Oid
	// The data type size (see pg_type.typlen).
	// Note that negative values denote variable-width types.
	Len int
	// The type modifier (see pg_attribute.atttypmod).
	// The meaning of the modifier is type-specific.
	Mod int
}

func (fd fieldDesc) Type() reflect.Type {
	switch fd.OID {
	case oid.T_int8:
		return reflect.TypeFor[int64]()
	case oid.T_int4:
		return reflect.TypeFor[int32]()
	case oid.T_int2:
		return reflect.TypeFor[int16]()
	case oid.T_float8:
		return reflect.TypeFor[float64]()
	case oid.T_float4:
		return reflect.TypeFor[float32]()
	case oid.T_varchar, oid.T_text, oid.T_varbit, oid.T_bit:
		return reflect.TypeFor[string]()
	case oid.T_bool:
		return reflect.TypeFor[bool]()
	case oid.T_date, oid.T_time, oid.T_timetz, oid.T_timestamp, oid.T_timestamptz:
		return reflect.TypeFor[time.Time]()
	case oid.T_bytea:
		return reflect.TypeFor[[]byte]()
	default:
		return reflect.TypeFor[any]()
	}
}

func (fd fieldDesc) Name() string {
	return oid.TypeName[fd.OID]
}

func (fd fieldDesc) Length() (length int64, ok bool) {
	switch fd.OID {
	case oid.T_text, oid.T_bytea:
		return math.MaxInt64, true
	case oid.T_varchar, oid.T_bpchar:
		return int64(fd.Mod - headerSize), true
	case oid.T_varbit, oid.T_bit:
		return int64(fd.Mod), true
	default:
		return 0, false
	}
}

func (fd fieldDesc) PrecisionScale() (precision, scale int64, ok bool) {
	switch fd.OID {
	case oid.T_numeric, oid.T__numeric:
		mod := fd.Mod - headerSize
		precision = int64((mod >> 16) & 0xffff)
		scale = int64(mod & 0xffff)
		return precision, scale, true
	default:
		return 0, 0, false
	}
}

var redshiftTypeName = map[oid.Oid]string{
	86:   "PG_SHADOW",
	87:   "PG_GROUP",
	88:   "PG_DATABASE",
	90:   "PG_TABLESPACE",
	635:  "_SPECTRUM_ARRAY",
	636:  "_SPECTRUM_MAP",
	637:  "_SPECTRUM_STRUCT",
	1188: "INTERVALY2M",
	1189: "_INTERVALY2M",
	1190: "INTERVALD2S",
	1191: "_INTERVALD2S",
	2935: "HLLSKETCH",
	3000: "GEOMETRY",
	3001: "GEOGRAPHY",
	4000: "SUPER",
	4600: "USERITEM",
	4601: "_USERITEM",
	4602: "ROLEITEM",
	4603: "_ROLEITEM",
	6551: "VARBYTE",
}
