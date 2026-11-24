package pq

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	"github.com/lib/pq/oid"
)

// encodeParameter is the single decision point for a Bind parameter.  The OID
// is the one negotiated by Parse/Describe.  When preferBinary is false the
// result is always text.  With binary parameters enabled, []byte keeps its
// historical raw-binary meaning and known built-in arrays are emitted in the
// binary array format.  Unknown and custom types conservatively stay text and
// return formatText (the Bind format marker).
//
// A nil slice/value returns a nil byte slice; callers write Bind length -1.
// An empty but non-nil array produces a non-nil payload.
func encodeParameter(x any, negotiated oid.Oid, opts ...any) ([]byte, format, error) {
	switch nv := x.(type) {
	case driver.NamedValue:
		x = nv.Value
	case *driver.NamedValue:
		if nv == nil {
			x = nil
		} else {
			x = nv.Value
		}
	}

	binary := false
	if len(opts) > 0 {
		switch opt := opts[0].(type) {
		case bool:
			binary = opt
		case format:
			binary = opt == formatBinary
		}
	}

	if b, ok := x.([]byte); ok && binary {
		// Preserve binary_parameters behavior: even a typed nil []byte has
		// binary format, although its Bind value length is -1 (SQL NULL).
		return b, formatBinary, nil
	}
	if x == nil {
		return nil, formatText, nil
	}
	if b, ok := x.([]byte); ok && b == nil {
		return nil, formatText, nil
	}

	text, err := encode(x, negotiated)
	if err != nil {
		return nil, formatText, err
	}
	if text == nil {
		return nil, formatText, nil
	}

	if binary && negotiated != 0 && arrayElementOID(negotiated) != 0 && looksLikeTextArray(text) {
		if bin, err := binaryEncodeTextArray(text, negotiated); err == nil {
			return bin, formatBinary, nil
		}
		// If the element type has no safe binary representation, send the
		// canonical text array and mark format code zero.
	}
	return text, formatText, nil
}

// encodeParam is a short alias for callers that use the abbreviated name.
var encodeParam = encodeParameter

// binaryEncodeParameter is the binary-format specialization of
// encodeParameter.
func binaryEncodeParameter(x any, negotiated oid.Oid) ([]byte, format, error) {
	return encodeParameter(x, negotiated, formatBinary)
}

// encodeFallback handles named scalar types, driver.Valuer values, and Go
// arrays/slices not covered by the historical fast path in [encode].  The OID
// always comes from server negotiation; it is only used to choose a compatible
// encoding (for example bytea versus text).
func encodeFallback(x any, pgtypOid oid.Oid) ([]byte, error) {
	if x == nil {
		return nil, nil
	}

	if v, ok := x.(driver.Valuer); ok {
		value, err := v.Value()
		if err != nil {
			return nil, err
		}
		return encode(value, pgtypOid)
	}

	rv := reflect.ValueOf(x)
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, nil
	}

	// Named []byte kinds (for example json.RawMessage) must stay scalar byte
	// values rather than being treated as uint8 arrays.
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		if rv.IsNil() {
			return nil, nil
		}
		return encode(rv.Bytes(), pgtypOid)
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		value, err := GenericArray{A: rv.Interface()}.Value()
		if err != nil {
			return nil, err
		}
		switch value := value.(type) {
		case nil:
			return nil, nil
		case string:
			return encode(value, pgtypOid)
		case []byte:
			return encode(value, pgtypOid)
		default:
			return nil, fmt.Errorf("pq: encode: unsupported array value %T", value)
		}
	}

	if rv.CanInterface() {
		switch value := rv.Interface().(type) {
		case string:
			return []byte(value), nil
		case bool:
			return encode(value, pgtypOid)
		case int:
			return encode(int64(value), pgtypOid)
		case int8:
			return encode(int64(value), pgtypOid)
		case int16:
			return encode(int64(value), pgtypOid)
		case int32:
			return encode(int64(value), pgtypOid)
		case int64:
			return encode(value, pgtypOid)
		case uint:
			return encodeUint64(uint64(value))
		case uint8:
			return encodeUint64(uint64(value))
		case uint16:
			return encodeUint64(uint64(value))
		case uint32:
			return encodeUint64(uint64(value))
		case uint64:
			return encodeUint64(value)
		case float32:
			return encode(float64(value), pgtypOid)
		case float64:
			return encode(value, pgtypOid)
		case time.Time:
			return encode(value, pgtypOid)
		}

		switch rv.Kind() {
		case reflect.String:
			return []byte(rv.String()), nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return encode(rv.Int(), pgtypOid)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return encodeUint64(rv.Uint())
		case reflect.Float32, reflect.Float64:
			return encode(rv.Float(), pgtypOid)
		case reflect.Bool:
			return encode(rv.Bool(), pgtypOid)
		}
	}

	return nil, fmt.Errorf("pq: encode: unknown type for %T", x)
}

func encodeUint64(v uint64) ([]byte, error) {
	const maxInt64 = ^uint64(0) >> 1
	if v > maxInt64 {
		return []byte(fmt.Appendf(nil, "%d", v)), nil
	}
	return encode(int64(v), oid.T_unknown)
}
