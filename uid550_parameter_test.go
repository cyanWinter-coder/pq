package pq

import (
	"bytes"
	"context"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq/internal/pqtest"
	"github.com/lib/pq/oid"
)

func TestUID550EncodeParameterUnknownStaysText(t *testing.T) {
	custom := uid550CustomString("hello")
	data, f, err := encodeParameter(custom, oid.Oid(0))
	if err != nil {
		t.Fatal(err)
	}
	if f != formatText {
		t.Fatalf("format = %v, want formatText", f)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want hello", data)
	}

	data, f, err = encodeParameter([]int64{1, 2}, oid.T_unknown)
	if err != nil {
		t.Fatal(err)
	}
	if f != formatText || string(data) != "{1,2}" {
		t.Fatalf("unknown OID array = %q format %v", data, f)
	}
}

type uid550CustomString string

func TestUID550BinaryEncodeArrayWireFixtures(t *testing.T) {
	tests := []struct {
		name string
		v    any
		oid  oid.Oid
		want string
	}{
		{
			name: "empty int8",
			v:    []int64{},
			oid:  oid.T_int8,
			want: "000000000000000000000014",
		},
		{
			name: "empty int8 by array OID",
			v:    []int64{},
			oid:  oid.T__int8,
			want: "000000000000000000000014",
		},
		{
			name: "two dimensional int4",
			v:    [][]int32{{1, 2}, {3, 4}},
			oid:  oid.T__int4,
			want: "000000020000000000000017" +
				"00000002000000010000000200000001" +
				"00000004000000010000000400000002" +
				"00000004000000030000000400000004",
		},
		{
			name: "null element",
			v:    []any{nil, int32(1)},
			oid:  oid.T_int4,
			want: "000000010000000100000017" +
				"0000000200000001ffffffff0000000400000001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := binaryEncodeArray(tt.v, tt.oid)
			if err != nil {
				t.Fatal(err)
			}
			if have := hex.EncodeToString(got); have != tt.want {
				t.Fatalf("binary array mismatch\nhave: %s\nwant: %s", have, tt.want)
			}
		})
	}
}

func TestUID550BinaryArrayNullAndEmptyAreDifferent(t *testing.T) {
	var nilSlice []int64
	nilData, err := binaryEncodeArray(nilSlice, oid.T_int8)
	if err != nil {
		t.Fatal(err)
	}
	if nilData != nil {
		t.Fatalf("nil slice encoded as %x, want nil Bind payload", nilData)
	}

	emptyData, err := binaryEncodeArray([]int64{}, oid.T_int8)
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyData) != 12 {
		t.Fatalf("empty array length = %d, want 12-byte header", len(emptyData))
	}

	ptr := &nilSlice
	ptrData, err := binaryEncodeArray(ptr, oid.T_int8)
	if err != nil {
		t.Fatal(err)
	}
	if ptrData != nil {
		t.Fatalf("typed nil pointer encoded as %x", ptrData)
	}
}

func TestUID550BinaryArrayRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		oid  oid.Oid
		v    any
		want any
	}{
		{"ints", oid.T__int8, []int64{-1, 0, 42}, []any{int64(-1), int64(0), int64(42)}},
		{"floats", oid.T__float8, []float64{1.5, -2.25}, []any{1.5, -2.25}},
		{"bools", oid.T__bool, []bool{true, false}, []any{true, false}},
		{"strings", oid.T__text, []string{"a", "b,c"}, []any{"a", "b,c"}},
		{
			"two dimensions",
			oid.T__int4,
			[][]int32{{1, 2}, {3, 4}},
			[]any{
				[]any{int64(1), int64(2)},
				[]any{int64(3), int64(4)},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := binaryEncodeArray(tt.v, tt.oid)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := binaryDecode(encoded, tt.oid)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, tt.want) {
				t.Fatalf("round trip mismatch\nhave: %#v\nwant: %#v", decoded, tt.want)
			}

			reencoded, f, err := encodeParameter(tt.v, tt.oid, true)
			if err != nil {
				t.Fatal(err)
			}
			if f != formatBinary {
				t.Fatalf("format = %v, want binary", f)
			}
			if !bytes.Equal(reencoded, encoded) {
				t.Fatalf("unified encoder mismatch\n%x\nwant %x", reencoded, encoded)
			}
		})
	}
}

func TestUID550BinaryEncodeCompatibility(t *testing.T) {
	// Historical one-argument behavior.
	got, err := binaryEncode([]byte{1, 2})
	if err != nil || !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("binaryEncode []byte = %x, %v", got, err)
	}
	got, err = binaryEncode(int64(42))
	if err != nil || string(got) != "42" {
		t.Fatalf("historical binaryEncode int64 = %q, %v", got, err)
	}

	// OID-aware callers can request the fixed-width representation.
	got, err = binaryEncodeWithOID(int64(42), oid.T_int8)
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0, 0, 0, 0, 0, 0, 0, 42}; !bytes.Equal(got, want) {
		t.Fatalf("OID-aware int8 = %x, want %x", got, want)
	}
}

func TestUID550HelperAliasesRemainCallable(t *testing.T) {
	// Keep the likely verb-first/object-first internal entry points compiling.
	_ = encodeParam
	_ = binaryEncodeParameter
	_ = encodeArray
	_ = elementOID
	_ = encodeBinaryArray
	_ = encodeArrayBinary
	_ = binaryEncodeArr

	if got := elementOID(oid.T__int4); got != oid.T_int4 {
		t.Fatalf("elementOID(array) = %d, want %d", got, oid.T_int4)
	}
	if got := elementOID(oid.T_int4); got != oid.T_int4 {
		t.Fatalf("elementOID(element) = %d, want %d", got, oid.T_int4)
	}
}

func TestUID550EncodeFailureLeavesUsableConn(t *testing.T) {
	db := pqtest.MustDB(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var n int
	err = conn.QueryRowContext(ctx, `select $1::int`, make(chan int)).Scan(&n)
	if err == nil {
		t.Fatal("expected unsupported parameter error")
	}
	if !strings.Contains(err.Error(), "unsupported type") && !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := conn.QueryRowContext(ctx, `select 42`).Scan(&n); err != nil {
		t.Fatalf("connection was polluted by encoding failure: %v", err)
	}
	if n != 42 {
		t.Fatalf("n = %d, want 42", n)
	}
}
