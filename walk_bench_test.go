package rueidis

import (
	"context"
	"strconv"
	"testing"
)

const benchAddr = "127.0.0.1:6379"

var benchSink [][]byte

func benchClient(b *testing.B, opt ClientOption) Client {
	b.Helper()
	if len(opt.InitAddress) == 0 {
		opt.InitAddress = []string{benchAddr}
	}
	c, err := NewClient(opt)
	if err != nil {
		b.Skipf("redis not available: %v", err)
	}
	return c
}

func seedStream(b *testing.B, c Client, key string, entries int) {
	b.Helper()
	ctx := context.Background()
	c.Do(ctx, c.B().Del().Key(key).Build())
	for i := 0; i < entries; i++ {
		if err := c.Do(ctx, c.B().Xadd().Key(key).Id("*").
			FieldValue().FieldValue("field1", "value"+strconv.Itoa(i)).Build()).Error(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWalkXRange compares building the full message tree against
// walking it, on a 1000 entry XRANGE reply. Both variants copy every value out,
// so the only difference is the intermediate representation.
func BenchmarkWalkXRange(b *testing.B) {
	const entries = 1000
	c := benchClient(b, ClientOption{})
	defer c.Close()
	ctx := context.Background()
	key := "benchmark:xrange:stream"
	seedStream(b, c, key, entries)
	defer c.Do(ctx, c.B().Del().Key(key).Build())

	cmd := func() Completed {
		return c.B().Xrange().Key(key).Start("-").End("+").Build()
	}

	b.Run("Do_AsXRangeSlices", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r := c.Do(ctx, cmd())
			es, err := r.AsXRangeSlices()
			if err != nil {
				b.Fatal(err)
			}
			if len(es) != entries {
				b.Fatalf("got %d entries", len(es))
			}
			out := make([][]byte, 0, len(es))
			for i := range es {
				for _, fv := range es[i].FieldValues {
					if fv.Field == "field1" {
						out = append(out, []byte(fv.Value))
					}
				}
			}
			benchSink = out
		}
	})

	b.Run("Walk_perValue", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out [][]byte
			s := c.DoStream(ctx, cmd())
			err := s.Walk(func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0: // the entry array
					out = make([][]byte, 0, i.IntVal)
					return i.IsAggregate(), nil
				case 1: // [id, fields]
					return i.IsAggregate(), nil
				case 2: // index 0 is the id, index 1 the field array
					return i.Index == 1 && i.IsAggregate(), nil
				case 3: // field name then value
					if i.Index%2 == 1 && i.IsString() {
						out = append(out, append([]byte(nil), i.Peek...))
					}
				}
				return false, nil
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(out) != entries {
				b.Fatalf("got %d entries", len(out))
			}
			benchSink = out
		}
	})

	// Isolates the walk's own cost: descend the same tree, touch every value,
	// but copy nothing out.
	b.Run("Walk_noCopy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var n, sum int
			s := c.DoStream(ctx, cmd())
			err := s.Walk(func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0, 1:
					return i.IsAggregate(), nil
				case 2:
					return i.Index == 1 && i.IsAggregate(), nil
				case 3:
					if i.Index%2 == 1 && i.IsString() {
						n++
						sum += len(i.Peek)
					}
				}
				return false, nil
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if n != entries || sum == 0 {
				b.Fatalf("got %d values", n)
			}
		}
	})

	// Copies every value out, but into one growing buffer with an offset table
	// rather than a slice per value.
	b.Run("Walk_arena", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var arena []byte
			var offs []int32
			s := c.DoStream(ctx, cmd())
			err := s.Walk(func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0:
					arena = make([]byte, 0, i.IntVal*16)
					offs = make([]int32, 0, i.IntVal+1)
					offs = append(offs, 0)
					return i.IsAggregate(), nil
				case 1:
					return i.IsAggregate(), nil
				case 2:
					return i.Index == 1 && i.IsAggregate(), nil
				case 3:
					if i.Index%2 == 1 && i.IsString() {
						arena = append(arena, i.Peek...)
						offs = append(offs, int32(len(arena)))
					}
				}
				return false, nil
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(offs) != entries+1 {
				b.Fatalf("got %d values", len(offs)-1)
			}
			benchSink = [][]byte{arena}
		}
	})
}

// BenchmarkWalkXRead is the same comparison against an XREAD reply, whose
// RESP3 shape is a map of stream name to entries.
func BenchmarkWalkXRead(b *testing.B) {
	const entries = 1000
	c := benchClient(b, ClientOption{})
	defer c.Close()
	ctx := context.Background()
	key := "benchmark:xread:stream"
	seedStream(b, c, key, entries)
	defer c.Do(ctx, c.B().Del().Key(key).Build())

	cmd := func() Completed {
		return c.B().Xread().Count(int64(entries)).Streams().Key(key).Id("0-0").Build()
	}

	b.Run("Do_AsXRead", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r := c.Do(ctx, cmd())
			m, err := r.AsXRead()
			if err != nil {
				b.Fatal(err)
			}
			var out [][]byte
			for _, es := range m {
				for i := range es {
					out = append(out, []byte(es[i].FieldValues["field1"]))
				}
			}
			if len(out) != entries {
				b.Fatalf("got %d entries", len(out))
			}
			benchSink = out
		}
	})

	b.Run("Walk_perValue", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out [][]byte
			// This descends the RESP3 shape, where the reply is a map of
			// stream name to entries. RESP2 nests each stream in an extra
			// array, one level deeper; rueidis speaks RESP3 to any server
			// that supports it, which the benchmark relies on.
			s := c.DoStream(ctx, cmd())
			err := s.Walk(func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0: // map of stream name to entries
					return i.IsAggregate(), nil
				case 1: // index 0 is the stream name, index 1 the entries
					if i.Index == 1 && i.IsAggregate() {
						out = make([][]byte, 0, i.IntVal)
						return true, nil
					}
				case 2: // [id, fields]
					return i.IsAggregate(), nil
				case 3:
					return i.Index == 1 && i.IsAggregate(), nil
				case 4:
					if i.Index%2 == 1 && i.IsString() {
						out = append(out, append([]byte(nil), i.Peek...))
					}
				}
				return false, nil
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(out) != entries {
				b.Fatalf("got %d entries", len(out))
			}
			benchSink = out
		}
	})

}

// benchLuaScript returns {offset, epoch, pubs} where pubs is an XRANGE result,
// a shape close to what a real application script produces: a few scalars at
// the top level followed by a large nested array.
const benchLuaScript = `
local stream_key = KEYS[1]
local meta_key = KEYS[2]
local stream_meta = redis.call("hmget", meta_key, "e", "o")
local epoch = stream_meta[1]
local offset = stream_meta[2]
if epoch == false then epoch = "v1" offset = 0 end
if offset == false then offset = 0 end
local pubs = redis.call("xrange", stream_key, ARGV[1], "+", "COUNT", ARGV[2])
return { offset, epoch, pubs }
`

// BenchmarkWalkLua measures a mixed reply: scalars that need parsing plus a
// large nested array, extracted the way an application would.
func BenchmarkWalkLua(b *testing.B) {
	const entries = 100
	c := benchClient(b, ClientOption{})
	defer c.Close()
	ctx := context.Background()
	streamKey, metaKey := "benchmark:lua:stream", "benchmark:lua:meta"
	seedStream(b, c, streamKey, entries)
	if err := c.Do(ctx, c.B().Hset().Key(metaKey).
		FieldValue().FieldValue("e", "v1").FieldValue("o", "100").Build()).Error(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		c.Do(ctx, c.B().Del().Key(streamKey).Build())
		c.Do(ctx, c.B().Del().Key(metaKey).Build())
	}()

	sha, err := c.Do(ctx, c.B().ScriptLoad().Script(benchLuaScript).Build()).ToString()
	if err != nil {
		b.Fatal(err)
	}
	limit := strconv.Itoa(entries)
	cmd := func() Completed {
		return c.B().Evalsha().Sha1(sha).Numkeys(2).
			Key(streamKey, metaKey).Arg("-", limit).Build()
	}

	b.Run("Do_Standard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r := c.Do(ctx, cmd())
			vs, err := r.ToArray()
			if err != nil {
				b.Fatal(err)
			}
			offset, err := vs[0].AsUint64()
			if err != nil {
				b.Fatal(err)
			}
			epoch, err := vs[1].ToString()
			if err != nil {
				b.Fatal(err)
			}
			pubs, err := vs[2].ToArray()
			if err != nil {
				b.Fatal(err)
			}
			out := make([][]byte, 0, len(pubs))
			for i := range pubs {
				e, err := pubs[i].ToArray()
				if err != nil {
					b.Fatal(err)
				}
				fs, err := e[1].ToArray()
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j+1 < len(fs); j += 2 {
					k, _ := fs[j].ToString()
					if k == "field1" {
						v, _ := fs[j+1].ToString()
						out = append(out, []byte(v))
					}
				}
			}
			if offset == 0 || epoch == "" || len(out) != entries {
				b.Fatalf("invalid result: %d %q %d", offset, epoch, len(out))
			}
			benchSink = out
		}
	})

	b.Run("Walk_perValue", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var offset uint64
			var epoch string
			var out [][]byte
			s := c.DoStream(ctx, cmd())
			err := s.Walk(func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0:
					return i.IsAggregate(), nil
				case 1:
					switch {
					case i.Index == 0 && i.IsInt():
						offset = uint64(i.IntVal)
					case i.Index == 0 && i.IsString():
						// The error can be returned now rather than stashed.
						v, err := strconv.ParseUint(string(i.Peek), 10, 64)
						if err != nil {
							return false, err
						}
						offset = v
					case i.Index == 1 && i.IsString():
						epoch = string(i.Peek)
					case i.Index == 2 && i.IsAggregate():
						out = make([][]byte, 0, i.IntVal)
						return true, nil
					}
				case 2:
					return i.IsAggregate(), nil
				case 3:
					return i.Index == 1 && i.IsAggregate(), nil
				case 4:
					if i.Index%2 == 1 && i.IsString() {
						out = append(out, append([]byte(nil), i.Peek...))
					}
				}
				return false, nil
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if offset == 0 || epoch == "" || len(out) != entries {
				b.Fatalf("invalid result: %d %q %d", offset, epoch, len(out))
			}
			benchSink = out
		}
	})
}

func seedHash(b *testing.B, c Client, key string, fields int) {
	b.Helper()
	ctx := context.Background()
	c.Do(ctx, c.B().Del().Key(key).Build())
	cb := c.B().Hset().Key(key).FieldValue()
	for i := 0; i < fields; i++ {
		cb = cb.FieldValue("field"+strconv.Itoa(i), strconv.Itoa(i))
	}
	if err := c.Do(ctx, cb.Build()).Error(); err != nil {
		b.Fatal(err)
	}
}

// atoiPeek parses a base-10 integer straight from a borrowed Peek, without
// allocating a string.
func atoiPeek(p []byte) int64 {
	var n int64
	for _, c := range p {
		n = n*10 + int64(c-'0')
	}
	return n
}

// BenchmarkWalkHGetAll contrasts the two ways an application uses a large hash.
// HGETALL is protocol-independent — a RESP3 map and a RESP2 array both present
// k,v,k,v at the same depth and index — so the same callback serves both via
// Walk(fn, fn).
//
// Counting/aggregating (keep only a scalar): the map Do builds is pure
// overhead, and Walk wins big. Building a map you retain: both copy every key
// and value, so it is roughly break-even — the walk's edge is only the reply
// tree it skips.
func BenchmarkWalkHGetAll(b *testing.B) {
	const fields = 1000
	c := benchClient(b, ClientOption{})
	defer c.Close()
	ctx := context.Background()
	key := "benchmark:hgetall:stream"
	seedHash(b, c, key, fields)
	defer c.Do(ctx, c.B().Del().Key(key).Build())
	cmd := func() Completed { return c.B().Hgetall().Key(key).Build() }

	// --- count fields whose value is even; keep only the counter ---

	b.Run("Do_CountEven", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m, err := c.Do(ctx, cmd()).AsStrMap()
			if err != nil {
				b.Fatal(err)
			}
			cnt := 0
			for _, v := range m {
				n, _ := strconv.Atoi(v)
				if n%2 == 0 {
					cnt++
				}
			}
			if cnt == 0 {
				b.Fatal("zero")
			}
		}
	})

	b.Run("Walk_CountEven", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cnt := 0
			walk := func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0:
					return true, nil // descend map (RESP3) or array (RESP2)
				case 1:
					if i.Index%2 == 1 && atoiPeek(i.Peek)%2 == 0 { // values are odd-indexed
						cnt++
					}
				}
				return false, nil
			}
			s := c.DoStream(ctx, cmd())
			if err := s.Walk(walk, walk); err != nil { // same fn, both protocols
				b.Fatal(err)
			}
			if cnt == 0 {
				b.Fatal("zero")
			}
		}
	})

	// --- build a map[string]string the caller retains ---

	b.Run("Do_ToMap", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m, err := c.Do(ctx, cmd()).AsStrMap()
			if err != nil {
				b.Fatal(err)
			}
			if len(m) != fields {
				b.Fatalf("got %d", len(m))
			}
		}
	})

	b.Run("Walk_ToMap", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m := make(map[string]string, fields)
			var field string
			walk := func(i WalkInfo) (bool, error) {
				switch i.Depth {
				case 0:
					return true, nil
				case 1:
					if i.Index%2 == 0 {
						field = string(i.Peek) // copy the key out of the buffer
					} else {
						m[field] = string(i.Peek) // copy the value out of the buffer
					}
				}
				return false, nil
			}
			s := c.DoStream(ctx, cmd())
			if err := s.Walk(walk, walk); err != nil {
				b.Fatal(err)
			}
			if len(m) != fields {
				b.Fatalf("got %d", len(m))
			}
			benchSink = nil
		}
	})
}

// BenchmarkWalkHGetAllScaling shows how the count workload scales with reply
// size: Do's cost grows with the field count, the walk's own cost is flat.
func BenchmarkWalkHGetAllScaling(b *testing.B) {
	c := benchClient(b, ClientOption{})
	defer c.Close()
	ctx := context.Background()
	for _, size := range []int{100, 1000, 10000} {
		key := "benchmark:hgetall:scale:" + strconv.Itoa(size)
		seedHash(b, c, key, size)
		cmd := func() Completed { return c.B().Hgetall().Key(key).Build() }

		b.Run("Do_CountEven/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				m, err := c.Do(ctx, cmd()).AsStrMap()
				if err != nil {
					b.Fatal(err)
				}
				cnt := 0
				for _, v := range m {
					n, _ := strconv.Atoi(v)
					if n%2 == 0 {
						cnt++
					}
				}
				if cnt == 0 {
					b.Fatal("zero")
				}
			}
		})

		b.Run("Walk_CountEven/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				cnt := 0
				walk := func(i WalkInfo) (bool, error) {
					switch i.Depth {
					case 0:
						return true, nil
					case 1:
						if i.Index%2 == 1 && atoiPeek(i.Peek)%2 == 0 {
							cnt++
						}
					}
					return false, nil
				}
				s := c.DoStream(ctx, cmd())
				if err := s.Walk(walk, walk); err != nil {
					b.Fatal(err)
				}
				if cnt == 0 {
					b.Fatal("zero")
				}
			}
		})
		c.Do(ctx, c.B().Del().Key(key).Build())
	}
}
