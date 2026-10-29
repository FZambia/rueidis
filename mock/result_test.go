package mock

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/rueidis"
)

func TestRedisString(t *testing.T) {
	m := RedisString("s")
	if v, err := m.ToString(); err != nil || v != "s" {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestRedisError(t *testing.T) {
	m := RedisError("s")
	if err := m.Error(); err == nil || err.Error() != "s" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestRedisInt64(t *testing.T) {
	m := RedisInt64(1)
	if v, err := m.ToInt64(); err != nil || v != int64(1) {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsInt64(); err != nil || v != int64(1) {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestRedisFloat64(t *testing.T) {
	m := RedisFloat64(1)
	if v, err := m.ToFloat64(); err != nil || v != float64(1) {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsFloat64(); err != nil || v != float64(1) {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestRedisBool(t *testing.T) {
	m := RedisBool(true)
	if v, err := m.ToBool(); err != nil || v != true {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsBool(); err != nil || v != true {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestRedisNil(t *testing.T) {
	m := RedisNil()
	if err := m.Error(); err == nil {
		t.Fatalf("unexpected value %v", err)
	}
	if v := m.IsNil(); v != true {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestRedisArray(t *testing.T) {
	m := RedisArray(RedisString("0"), RedisString("1"), RedisString("2"))
	if arr, err := m.AsStrSlice(); err != nil || !reflect.DeepEqual(arr, []string{"0", "1", "2"}) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestRedisMap(t *testing.T) {
	m := RedisMap(map[string]rueidis.RedisMessage{
		"a": RedisString("0"),
		"b": RedisString("1"),
	})
	if arr, err := m.AsStrMap(); err != nil || !reflect.DeepEqual(arr, map[string]string{
		"a": "0",
		"b": "1",
	}) {
		t.Fatalf("unexpected value %v", err)
	}
	if arr, err := m.ToMap(); err != nil || !reflect.DeepEqual(arr, map[string]rueidis.RedisMessage{
		"a": RedisString("0"),
		"b": RedisString("1"),
	}) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestRedisResult(t *testing.T) {
	r := Result(RedisNil())
	if err := r.Error(); !rueidis.IsRedisNil(err) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorResult(t *testing.T) {
	r := ErrorResult(errors.New("any"))
	if err := r.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorResultStream(t *testing.T) {
	s := RedisResultStreamError(errors.New("any"))
	if err := s.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorMultiResultStream(t *testing.T) {
	s := MultiRedisResultStreamError(errors.New("any"))
	if err := s.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestResultStream(t *testing.T) {
	type test struct {
		msg []rueidis.RedisMessage
		out []string
		err []string
	}
	tests := []test{
		{msg: []rueidis.RedisMessage{RedisString("")}, out: []string{""}, err: []string{""}},
		{msg: []rueidis.RedisMessage{RedisString("0"), RedisBlobString("12345")}, out: []string{"0", "12345"}, err: []string{"", ""}},
		{msg: []rueidis.RedisMessage{RedisInt64(123), RedisInt64(-456)}, out: []string{"123", "-456"}, err: []string{"", ""}},
		{msg: []rueidis.RedisMessage{RedisString(""), RedisNil()}, out: []string{"", ""}, err: []string{"", "nil"}},
		{msg: []rueidis.RedisMessage{RedisArray(RedisString("n")), RedisString("ok"), RedisNil(), RedisMap(map[string]rueidis.RedisMessage{"b": RedisBlobString("b")})}, out: []string{"", "ok", "", ""}, err: []string{"unsupported", "", "nil", "unsupported"}},
	}
	for _, tc := range tests {
		s := RedisResultStream(tc.msg...)
		if err := s.Error(); err != nil {
			t.Fatalf("unexpected value %v", err)
		}
		if !s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		buf := bytes.NewBuffer(nil)
		for i := 0; s.HasNext(); i++ {
			n, err := s.WriteTo(buf)
			if tc.err[i] != "" {
				if err == nil {
					t.Fatalf("unexpected value %v", err)
				} else if !strings.Contains(err.Error(), tc.err[i]) {
					t.Fatalf("unexpected value %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected value %v", err)
			}
			if n != int64(len(tc.out[i])) {
				t.Fatalf("unexpected value %v", n)
			}
		}
		if buf.String() != strings.Join(tc.out, "") {
			t.Fatalf("unexpected value %v", buf.String())
		}
		if s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		if err := s.Error(); err != io.EOF {
			t.Fatalf("unexpected value %v", err)
		}
	}
}

func TestMultiResultStream(t *testing.T) {
	type test struct {
		msg []rueidis.RedisMessage
		out []string
		err []string
	}
	tests := []test{
		{msg: []rueidis.RedisMessage{RedisString("")}, out: []string{""}, err: []string{""}},
		{msg: []rueidis.RedisMessage{RedisString("0"), RedisBlobString("12345")}, out: []string{"0", "12345"}, err: []string{"", ""}},
		{msg: []rueidis.RedisMessage{RedisInt64(123), RedisInt64(-456)}, out: []string{"123", "-456"}, err: []string{"", ""}},
		{msg: []rueidis.RedisMessage{RedisString(""), RedisNil()}, out: []string{"", ""}, err: []string{"", "nil"}},
		{msg: []rueidis.RedisMessage{RedisArray(RedisString("n")), RedisString("ok"), RedisNil(), RedisMap(map[string]rueidis.RedisMessage{"b": RedisBlobString("b")})}, out: []string{"", "ok", "", ""}, err: []string{"unsupported", "", "nil", "unsupported"}},
	}
	for _, tc := range tests {
		s := MultiRedisResultStream(tc.msg...)
		if err := s.Error(); err != nil {
			t.Fatalf("unexpected value %v", err)
		}
		if !s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		buf := bytes.NewBuffer(nil)
		for i := 0; s.HasNext(); i++ {
			n, err := s.WriteTo(buf)
			if tc.err[i] != "" {
				if err == nil {
					t.Fatalf("unexpected value %v", err)
				} else if !strings.Contains(err.Error(), tc.err[i]) {
					t.Fatalf("unexpected value %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected value %v", err)
			}
			if n != int64(len(tc.out[i])) {
				t.Fatalf("unexpected value %v", n)
			}
		}
		if buf.String() != strings.Join(tc.out, "") {
			t.Fatalf("unexpected value %v", buf.String())
		}
		if s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		if err := s.Error(); err != io.EOF {
			t.Fatalf("unexpected value %v", err)
		}
	}
}

// The pipe here is allocated from mock's definition but read and written by
// rueidis at the offsets of rueidis.pipe, so a field added to one and not the
// other silently misaligns everything behind it and pushes the tail fields past
// the end of this allocation. That is memory corruption with no build error and
// no obvious symptom, so compare the two definitions field by field. The types
// deliberately differ where mock cannot name an unexported one (queue, subs,
// CacheStore); every substitute is the same width, so the names in order are
// what has to match.
func TestPipeMirrorsRueidisPipe(t *testing.T) {
	const src = "../pipe.go" // reachable through the replace directive in go.mod
	f, err := parser.ParseFile(token.NewFileSet(), src, nil, 0)
	if err != nil {
		t.Skipf("cannot read %s: %v", src, err)
	}

	var want []string
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "pipe" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, field := range st.Fields.List {
			for _, name := range field.Names {
				want = append(want, name.Name)
			}
		}
		return false
	})
	if len(want) == 0 {
		t.Fatalf("no rueidis.pipe fields found in %s", src)
	}

	got := make([]string, 0, len(want))
	for i, typ := 0, reflect.TypeOf(pipe{}); i < typ.NumField(); i++ {
		got = append(got, typ.Field(i).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mock.pipe has drifted from rueidis.pipe\n mock: %v\nrueidis: %v", got, want)
	}
}

// TestPoolMirrorsRueidisPool guards the mock pool against the same silent
// misalignment TestPipeMirrorsRueidisPipe guards the pipe against: the mock
// stream stores its wire back into a *pool via an unsafe cast, so a reordered
// field would corrupt memory with no build error. Compare the field names in
// order; the substitute types (wire -> any, and the make/list element types)
// are the same width.
func TestPoolMirrorsRueidisPool(t *testing.T) {
	const src = "../pool.go" // reachable through the replace directive in go.mod
	f, err := parser.ParseFile(token.NewFileSet(), src, nil, 0)
	if err != nil {
		t.Skipf("cannot read %s: %v", src, err)
	}

	var want []string
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "pool" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, field := range st.Fields.List {
			for _, name := range field.Names {
				want = append(want, name.Name)
			}
		}
		return false
	})
	if len(want) == 0 {
		t.Fatalf("no rueidis.pool fields found in %s", src)
	}

	got := make([]string, 0, len(want))
	for i, typ := 0, reflect.TypeOf(pool{}); i < typ.NumField(); i++ {
		got = append(got, typ.Field(i).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mock.pool has drifted from rueidis.pool\n mock: %v\nrueidis: %v", got, want)
	}
}
