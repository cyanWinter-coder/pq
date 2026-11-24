package pq

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/lib/pq/oid"
)

// oidByName is the reverse of the generated oid.TypeName mapping.
var oidByName = func() map[string]oid.Oid {
	m := make(map[string]oid.Oid, len(oid.TypeName))
	for id, name := range oid.TypeName {
		m[name] = id
	}
	return m
}()

// arrayElementOID returns the element OID for a known PostgreSQL array OID.
// Custom types are not in the generated catalog map and return zero; callers
// must keep those values in text format.
func arrayElementOID(arrayOID oid.Oid) oid.Oid {
	name := oid.TypeName[arrayOID]
	if !strings.HasPrefix(name, "_") {
		return 0
	}
	return oidByName[name[1:]]
}

// elementOID normalizes either an array OID or an element OID to the element
// OID.  Unknown OIDs are returned unchanged.
func elementOID(id oid.Oid) oid.Oid {
	if elem := arrayElementOID(id); elem != 0 {
		return elem
	}
	return id
}

// binaryEncodeArray encodes a Go slice or array using PostgreSQL's binary
// array representation.  elementOID may be zero, in which case it is inferred
// from the Go element type.  A nil slice returns nil so callers can preserve
// the SQL NULL versus empty-array distinction.
func binaryEncodeArray(v any, elementOID oid.Oid) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, nil
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("pq: cannot encode %T as a PostgreSQL array", v)
	}
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return nil, nil
	}

	if elementOID != 0 && elementOID != oid.T_unknown {
		// Allow callers to pass either the array OID (e.g. T__int4=1007) or
		// its element OID (T_int4=23).  The header always carries the latter.
		if elem := arrayElementOID(elementOID); elem != 0 {
			elementOID = elem
		}
	}
	if elementOID == 0 || elementOID == oid.T_unknown {
		elementOID = reflectElementOID(rv.Type().Elem())
	}
	if elementOID == 0 || elementOID == oid.T_unknown {
		return nil, fmt.Errorf("pq: unable to determine OID for binary array element of type %T", v)
	}

	dims, err := binaryArrayShape(rv)
	if err != nil {
		return nil, err
	}

	var payload []byte
	hasNulls, err := appendBinaryArrayElements(&payload, rv, elementOID)
	if err != nil {
		return nil, err
	}

	// An empty array is represented with zero dimensions, but still carries
	// its element OID.  Non-empty values carry one entry per dimension.
	ndim := len(dims)
	total := 1
	for _, n := range dims {
		total *= n
	}
	if total == 0 {
		ndim = 0
	}

	out := make([]byte, 0, 20+8*len(dims)+len(payload))
	out = binary.BigEndian.AppendUint32(out, uint32(ndim))
	flags := uint32(0)
	if hasNulls {
		flags = 1
	}
	out = binary.BigEndian.AppendUint32(out, flags)
	out = binary.BigEndian.AppendUint32(out, uint32(elementOID))
	if ndim > 0 {
		for _, n := range dims {
			out = binary.BigEndian.AppendUint32(out, uint32(n))
			out = binary.BigEndian.AppendUint32(out, 1) // lower bound
		}
	}
	out = append(out, payload...)
	return out, nil
}

// Aliases for callers/tests that name the operation in verb-first or
// object-first style.
var (
	encodeBinaryArray = binaryEncodeArray
	encodeArrayBinary = binaryEncodeArray
	binaryEncodeArr   = binaryEncodeArray
)

// encodeArray accepts either an array OID or its element OID.  With no format
// option it returns PostgreSQL's binary array representation; false or
// formatText can be passed to request the historical text array form.
func encodeArray(x any, pgtypOid oid.Oid, opts ...any) ([]byte, error) {
	binary := true
	if len(opts) > 0 {
		switch opt := opts[0].(type) {
		case bool:
			binary = opt
		case format:
			binary = opt == formatBinary
		default:
			return nil, fmt.Errorf("pq: unsupported encodeArray option %T", opt)
		}
	}
	if !binary {
		v, err := GenericArray{A: x}.Value()
		if err != nil {
			return nil, err
		}
		switch v := v.(type) {
		case nil:
			return nil, nil
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			return nil, fmt.Errorf("pq: unsupported text array value %T", v)
		}
	}
	return binaryEncodeArray(x, pgtypOid)
}

func binaryArrayShape(rv reflect.Value) ([]int, error) {
	if !rv.IsValid() {
		return nil, nil
	}
	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			// A nil scalar pointer/interface has no further dimensions.  A
			// nil nested slice is diagnosed by sibling shape comparison or by
			// the element encoder's type information.
			return nil, nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, nil
	}
	// A slice/array of uint8 is a scalar bytea/uuid value, not a one-element
	// array dimension.
	if rv.Type().Elem().Kind() == reflect.Uint8 {
		return nil, nil
	}

	n := rv.Len()
	dims := []int{n}
	if n == 0 {
		return dims, nil
	}

	first, err := binaryArrayIndirect(rv.Index(0))
	if err != nil {
		return nil, err
	}
	sub, err := binaryArrayShape(first)
	if err != nil {
		return nil, err
	}
	dims = append(dims, sub...)

	for i := 1; i < n; i++ {
		child, err := binaryArrayIndirect(rv.Index(i))
		if err != nil {
			return nil, err
		}
		childDims, err := binaryArrayShape(child)
		if err != nil {
			return nil, err
		}
		if !equalInts(sub, childDims) {
			return nil, fmt.Errorf("pq: cannot encode ragged array: dimension 0 index %d has shape %v, want %v", i, childDims, sub)
		}
	}
	return dims, nil
}

func binaryArrayIndirect(rv reflect.Value) (reflect.Value, error) {
	for rv.IsValid() && (rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer) {
		if rv.IsNil() {
			return reflect.Value{}, nil
		}
		rv = rv.Elem()
	}
	return rv, nil
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func appendBinaryArrayElements(dst *[]byte, rv reflect.Value, elementOID oid.Oid) (bool, error) {
	rv, err := binaryArrayIndirect(rv)
	if err != nil {
		return false, err
	}
	if !rv.IsValid() {
		*dst = binary.BigEndian.AppendUint32(*dst, math.MaxUint32)
		return true, nil
	}

	// A byte slice is one scalar element (bytea/uuid-like), not a nested array.
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		if rv.IsNil() {
			*dst = binary.BigEndian.AppendUint32(*dst, math.MaxUint32)
			return true, nil
		}
		b, err := binaryEncodeScalar(rv, elementOID)
		if err != nil {
			return false, err
		}
		if b == nil {
			*dst = binary.BigEndian.AppendUint32(*dst, math.MaxUint32)
			return true, nil
		}
		*dst = binary.BigEndian.AppendUint32(*dst, uint32(len(b)))
		*dst = append(*dst, b...)
		return false, nil
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		hasNulls := false
		for i := 0; i < rv.Len(); i++ {
			null, err := appendBinaryArrayElements(dst, rv.Index(i), elementOID)
			if err != nil {
				return false, err
			}
			hasNulls = hasNulls || null
		}
		return hasNulls, nil
	}

	if isNullValue(rv) {
		*dst = binary.BigEndian.AppendUint32(*dst, math.MaxUint32)
		return true, nil
	}

	b, err := binaryEncodeScalar(rv, elementOID)
	if err != nil {
		return false, fmt.Errorf("pq: encoding array element %d: %w", 0, err)
	}
	if b == nil {
		*dst = binary.BigEndian.AppendUint32(*dst, math.MaxUint32)
		return true, nil
	}
	*dst = binary.BigEndian.AppendUint32(*dst, uint32(len(b)))
	*dst = append(*dst, b...)
	return false, nil
}

func isNullValue(rv reflect.Value) bool {
	if !rv.IsValid() {
		return true
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return rv.IsNil()
	}
	return false
}

func binaryEncodeValue(x any, typ oid.Oid) ([]byte, error) {
	if x == nil {
		return nil, nil
	}

	// Arrays and slices use the binary array header.  byte slices remain
	// scalar bytea/uuid payloads unless the negotiated OID is an array type.
	if typ != 0 && arrayElementOID(typ) != 0 {
		if rv := reflect.ValueOf(x); rv.IsValid() {
			kind := rv.Kind()
			for kind == reflect.Pointer || kind == reflect.Interface {
				if rv.IsNil() {
					return nil, nil
				}
				rv = rv.Elem()
				kind = rv.Kind()
			}
			if kind == reflect.Slice || kind == reflect.Array {
				if kind == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
					elemOID := arrayElementOID(typ)
					if elemOID != oid.T_bytea {
						return binaryEncodeArray(x, typ)
					}
				} else {
					return binaryEncodeArray(x, typ)
				}
			}
		}
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
	if b, ok := x.([]byte); ok {
		return b, nil
	}
	return binaryEncodeScalar(rv, typ)
}

func binaryEncodeScalar(rv reflect.Value, typ oid.Oid) ([]byte, error) {
	if rv.CanInterface() {
		x := rv.Interface()
		if bv, ok := x.(interface{ BinaryValue() ([]byte, error) }); ok {
			return bv.BinaryValue()
		}
		if v, ok := x.(driver.Valuer); ok {
			value, err := v.Value()
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, nil
			}
			rv = reflect.ValueOf(value)
		}
	}

	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}

	// The negotiated OID is authoritative when it identifies a fixed binary
	// representation.  Text-like OIDs share the same raw-byte representation.
	switch typ {
	case oid.T_bool:
		b, err := scalarBool(rv)
		if err != nil {
			return nil, err
		}
		if b {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case oid.T_int2, oid.T_int4, oid.T_int8, oid.T_oid:
		n, err := scalarInt64(rv)
		if err != nil {
			return nil, err
		}
		width := map[oid.Oid]int{oid.T_int2: 2, oid.T_int4: 4, oid.T_int8: 8, oid.T_oid: 4}[typ]
		out := make([]byte, width)
		switch width {
		case 2:
			binary.BigEndian.PutUint16(out, uint16(n))
		case 4:
			binary.BigEndian.PutUint32(out, uint32(n))
		default:
			binary.BigEndian.PutUint64(out, uint64(n))
		}
		return out, nil
	case oid.T_float4, oid.T_float8:
		f, err := scalarFloat64(rv)
		if err != nil {
			return nil, err
		}
		if typ == oid.T_float4 {
			out := make([]byte, 4)
			binary.BigEndian.PutUint32(out, math.Float32bits(float32(f)))
			return out, nil
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, math.Float64bits(f))
		return out, nil
	case oid.T_bytea:
		switch rv.Kind() {
		case reflect.Slice:
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				return rv.Bytes(), nil
			}
		case reflect.String:
			return []byte(rv.String()), nil
		}
		return nil, fmt.Errorf("pq: cannot encode %T as bytea", rv.Interface())
	case oid.T_uuid:
		return encodeUUIDBinary(rv)
	}

	switch rv.Kind() {
	case reflect.String:
		return []byte(rv.String()), nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Bytes(), nil
		}
	case reflect.Array:
		if rv.Type().Len() == 16 && rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, 16)
			reflect.Copy(reflect.ValueOf(b), rv)
			return b, nil
		}
	}

	if typ == 0 || typ == oid.T_unknown {
		return nil, fmt.Errorf("pq: no binary encoder for %T", rv.Interface())
	}
	// Many scalar types use the same binary and text representation as their
	// underlying string form.
	if rv.Kind() == reflect.String || (rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8) {
		return rv.Bytes(), nil
	}
	return nil, fmt.Errorf("pq: no binary encoder for %T as OID %d", rv.Interface(), typ)
}

func encodeUUIDBinary(rv reflect.Value) ([]byte, error) {
	switch rv.Kind() {
	case reflect.Array:
		if rv.Type().Len() != 16 || rv.Type().Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("pq: invalid UUID array type %s", rv.Type())
		}
		b := make([]byte, 16)
		reflect.Copy(reflect.ValueOf(b), rv)
		return b, nil
	case reflect.Slice:
		b := rv.Bytes()
		if len(b) == 16 {
			return b, nil
		}
		if len(b) == 36 {
			return parseUUIDToBinary(b)
		}
		return nil, fmt.Errorf("pq: unable to encode uuid; bad length: %d", len(b))
	case reflect.String:
		return parseUUIDToBinary([]byte(rv.String()))
	default:
		return nil, fmt.Errorf("pq: unable to encode uuid from %T", rv.Interface())
	}
}

func scalarBool(rv reflect.Value) (bool, error) {
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.String:
		switch strings.ToLower(rv.String()) {
		case "t", "true", "1":
			return true, nil
		case "f", "false", "0":
			return false, nil
		}
	}
	return false, fmt.Errorf("pq: cannot encode %T as bool", rv.Interface())
}

func scalarInt64(rv reflect.Value) (int64, error) {
	switch {
	case rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64:
		return rv.Int(), nil
	case rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint64:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return 0, fmt.Errorf("pq: uint64 value %d overflows PostgreSQL signed integer", u)
		}
		return int64(u), nil
	case rv.Kind() == reflect.Float32 || rv.Kind() == reflect.Float64:
		return int64(rv.Float()), nil
	case rv.Kind() == reflect.String:
		n, err := strconv.ParseInt(rv.String(), 10, 64)
		if err == nil {
			return n, nil
		}
	}
	return 0, fmt.Errorf("pq: cannot encode %T as integer", rv.Interface())
}

func scalarFloat64(rv reflect.Value) (float64, error) {
	switch {
	case rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64:
		return float64(rv.Int()), nil
	case rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint64:
		return float64(rv.Uint()), nil
	case rv.Kind() == reflect.Float32 || rv.Kind() == reflect.Float64:
		return rv.Float(), nil
	case rv.Kind() == reflect.String:
		return strconv.ParseFloat(rv.String(), 64)
	}
	return 0, fmt.Errorf("pq: cannot encode %T as float", rv.Interface())
}

func reflectElementOID(t reflect.Type) oid.Oid {

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Bool:
		return oid.T_bool
	case reflect.String:
		return oid.T_text
	case reflect.Int8:
		return oid.T_int2
	case reflect.Int16:
		return oid.T_int2
	case reflect.Int32:
		return oid.T_int4
	case reflect.Int, reflect.Int64:
		return oid.T_int8
	case reflect.Uint8:
		return 0 // Ambiguous byte versus uint8; require parent slice handling.
	case reflect.Uint16:
		return oid.T_int4
	case reflect.Uint32:
		return oid.T_int8
	case reflect.Uint, reflect.Uint64:
		return oid.T_int8
	case reflect.Float32:
		return oid.T_float4
	case reflect.Float64:
		return oid.T_float8
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return oid.T_bytea
		}
		return 0
	case reflect.Array:
		if t.Len() == 16 && t.Elem().Kind() == reflect.Uint8 {
			return oid.T_uuid
		}
		return 0
	default:
		return 0
	}
}

// binaryEncodeTextArray transcodes PostgreSQL text-array input into the binary
// array representation using the OID negotiated for the parameter.  It returns
// an error when the array type or an element has no safe binary encoding; the
// caller should then send the original text unchanged.
func binaryEncodeTextArray(src []byte, arrayOID oid.Oid) ([]byte, error) {
	elemOID := arrayElementOID(arrayOID)
	if elemOID == 0 {
		return nil, fmt.Errorf("pq: no negotiated element OID for array OID %d", arrayOID)
	}
	if string(src) == "{}" {
		return binaryEncodeArray([]any{}, elemOID)
	}

	dims, elems, err := parseArray(src, []byte{','})
	if err != nil {
		return nil, err
	}

	var payload []byte
	hasNulls := false
	for _, elem := range elems {
		if elem == nil {
			hasNulls = true
			payload = binary.BigEndian.AppendUint32(payload, math.MaxUint32)
			continue
		}
		raw, err := binaryEncodeTextElement(elem, elemOID)
		if err != nil {
			return nil, err
		}
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(raw)))
		payload = append(payload, raw...)
	}

	out := make([]byte, 0, 20+8*len(dims)+len(payload))
	out = binary.BigEndian.AppendUint32(out, uint32(len(dims)))
	flags := uint32(0)
	if hasNulls {
		flags = 1
	}
	out = binary.BigEndian.AppendUint32(out, flags)
	out = binary.BigEndian.AppendUint32(out, uint32(elemOID))
	for _, n := range dims {
		out = binary.BigEndian.AppendUint32(out, uint32(n))
		out = binary.BigEndian.AppendUint32(out, 1)
	}
	out = append(out, payload...)
	return out, nil
}

func binaryEncodeTextElement(src []byte, typ oid.Oid) ([]byte, error) {
	switch typ {
	case oid.T_bool:
		switch string(src) {
		case "t", "true", "1":
			return []byte{1}, nil
		case "f", "false", "0":
			return []byte{0}, nil
		default:
			return nil, fmt.Errorf("pq: invalid boolean %q", src)
		}
	case oid.T_int2:
		n, err := strconv.ParseInt(string(src), 10, 16)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 2)
		binary.BigEndian.PutUint16(out, uint16(n))
		return out, nil
	case oid.T_int4:
		n, err := strconv.ParseInt(string(src), 10, 32)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 4)
		binary.BigEndian.PutUint32(out, uint32(n))
		return out, nil
	case oid.T_int8, oid.T_oid:
		n, err := strconv.ParseInt(string(src), 10, 64)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(n))
		return out, nil
	case oid.T_float4:
		n, err := strconv.ParseFloat(string(src), 32)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 4)
		binary.BigEndian.PutUint32(out, math.Float32bits(float32(n)))
		return out, nil
	case oid.T_float8:
		n, err := strconv.ParseFloat(string(src), 64)
		if err != nil {
			return nil, err
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, math.Float64bits(n))
		return out, nil
	case oid.T_bytea:
		return parseBytea(src)
	case oid.T_uuid:
		return parseUUIDToBinary(src)
	case oid.T_text, oid.T_varchar, oid.T_bpchar, oid.T_char, oid.T_name:
		return src, nil
	default:
		return nil, fmt.Errorf("pq: no binary text element encoder for OID %d", typ)
	}
}

func parseUUIDToBinary(s []byte) ([]byte, error) {
	if len(s) != 36 {
		return nil, fmt.Errorf("pq: unable to encode uuid; bad length: %d", len(s))
	}
	out := make([]byte, 16)
	parts := [][]byte{s[0:8], s[9:13], s[14:18], s[19:23], s[24:36]}
	dst := out
	for _, p := range parts {
		n, err := hex.Decode(dst, p)
		if err != nil {
			return nil, err
		}
		dst = dst[n:]
	}
	return out, nil
}

// binaryDecodeArray parses PostgreSQL's binary array representation into
// nested []any values. nil elements are preserved as untyped nil.
func binaryDecodeArray(src []byte) (any, error) {
	if len(src) < 12 {
		return nil, fmt.Errorf("pq: short binary array header: %d bytes", len(src))
	}
	ndim := int(int32(binary.BigEndian.Uint32(src[0:4])))
	// flags := binary.BigEndian.Uint32(src[4:8])
	elemOID := oid.Oid(binary.BigEndian.Uint32(src[8:12]))
	pos := 12
	if ndim < 0 || ndim > 32 {
		return nil, fmt.Errorf("pq: invalid binary array dimension count: %d", ndim)
	}
	if len(src) < pos+8*ndim {
		return nil, fmt.Errorf("pq: short binary array dimension header")
	}
	dims := make([]int, ndim)
	for i := range ndim {
		dims[i] = int(int32(binary.BigEndian.Uint32(src[pos : pos+4])))
		// Lower bounds are accepted but do not affect the returned Go slice.
		pos += 8 // dimension size + lower bound
		if dims[i] < 0 {
			return nil, fmt.Errorf("pq: invalid binary array dimension size: %d", dims[i])
		}
	}

	// Zero-dimensional arrays are PostgreSQL's empty array.
	if ndim == 0 {
		return []any{}, nil
	}

	total := dims[0]
	for _, n := range dims[1:] {
		if n != 0 && total > math.MaxInt/n {
			return nil, fmt.Errorf("pq: binary array dimensions are too large")
		}
		total *= n
	}
	// Every encoded element has at least its four-byte length word.  This also
	// rejects dimension claims that would make the allocation overflow.
	if total < 0 || total > (len(src)-pos)/4 {
		return nil, fmt.Errorf("pq: binary array dimension count does not match payload")
	}
	values := make([]any, 0, total)
	for range total {
		if pos+4 > len(src) {
			return nil, fmt.Errorf("pq: short binary array element length")
		}
		l := int(int32(binary.BigEndian.Uint32(src[pos : pos+4])))
		pos += 4
		if l == -1 {
			values = append(values, nil)
			continue
		}
		if l < 0 || pos+l > len(src) {
			return nil, fmt.Errorf("pq: invalid binary array element length: %d", l)
		}
		raw := src[pos : pos+l]
		pos += l
		v, err := binaryDecodeArrayElement(raw, elemOID)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	if pos != len(src) {
		return nil, fmt.Errorf("pq: %d trailing bytes after binary array", len(src)-pos)
	}
	return nestBinaryArrayValues(values, dims), nil
}

func binaryDecodeArrayElement(raw []byte, elemOID oid.Oid) (any, error) {
	switch elemOID {
	case oid.T_uuid:
		return binaryDecode(raw, elemOID)
	default:
		v, err := binaryDecode(raw, elemOID)
		if err == nil {
			return v, nil
		}
		// Unknown/custom element types are returned verbatim rather than
		// making the whole array undecodable.
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, nil
	}
}

func nestBinaryArrayValues(values []any, dims []int) any {
	if len(dims) == 1 {
		out := make([]any, dims[0])
		copy(out, values)
		return out
	}
	width := 1
	for _, n := range dims[1:] {
		width *= n
	}
	out := make([]any, dims[0])
	for i := range dims[0] {
		out[i] = nestBinaryArrayValues(values[i*width:(i+1)*width], dims[1:])
	}
	return out
}
