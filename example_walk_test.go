package rueidis

import (
	"context"
	"fmt"
	"strings"
)

// Walking a reply reports every message with its depth and position, which is
// all that is needed to pick values out of it without building a message tree.
func ExampleRedisResultStream_Walk() {
	client, err := NewClient(ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	// XRANGE replies nest like this:
	//
	//   *N                     <- depth 0   the list of entries
	//     *2                   <- depth 1   one entry
	//       $  "1700000-0"     <- depth 2, index 0   the entry ID
	//       *2                 <- depth 2, index 1   the field list
	//         $  "field1"      <- depth 3, index 0   field name
	//         $  "value42"     <- depth 3, index 1   field value
	var values [][]byte

	s := client.DoStream(ctx, client.B().Xrange().Key("mystream").Start("-").End("+").Build())
	// The nil second argument declares this parser RESP3-only: on a RESP2
	// connection Walk returns ErrWalkRESP3Required instead of running it. To
	// serve RESP2 as well, pass a second callback — the same one again when the
	// command's reply shape is protocol-independent, as XRANGE's is.
	err = s.Walk(func(i WalkInfo) (bool, error) {
		switch i.Depth {
		case 0: // the list of entries; IntVal is the element count
			values = make([][]byte, 0, i.IntVal)
			return true, nil
		case 1: // one entry: [id, fields]
			return true, nil
		case 2: // skip the ID, descend into the field list
			return i.Index == 1, nil
		case 3: // field values sit at odd positions
			if i.Index%2 == 1 {
				// Peek points into the connection buffer and is only valid
				// during this call, so copy anything that is kept.
				values = append(values, append([]byte(nil), i.Peek...))
			}
		}
		return false, nil // anything declined is discarded without parsing
	}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(values))
}

// Retaining many values costs one allocation each. Appending them to a single
// buffer and recording where each ends keeps the total flat, a handful of
// allocations however many values are kept. The package benchmarks put
// numbers on the difference.
func ExampleRedisResultStream_Walk_singleBuffer() {
	client, err := NewClient(ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	var buf []byte   // every value, back to back
	var ends []int32 // where each value ends in buf

	s := client.DoStream(ctx, client.B().Xrange().Key("mystream").Start("-").End("+").Build())
	err = s.Walk(func(i WalkInfo) (bool, error) {
		switch i.Depth {
		case 0:
			buf = make([]byte, 0, i.IntVal*16)
			ends = make([]int32, 0, i.IntVal)
			return true, nil
		case 1:
			return true, nil
		case 2:
			return i.Index == 1, nil
		case 3:
			if i.Index%2 == 1 {
				buf = append(buf, i.Peek...)
				ends = append(ends, int32(len(buf)))
			}
		}
		return false, nil
	}, nil)
	if err != nil {
		panic(err)
	}

	// Reading a value back is a slice of buf and allocates nothing.
	value := func(n int) []byte {
		start := int32(0)
		if n > 0 {
			start = ends[n-1]
		}
		return buf[start:ends[n]]
	}
	if len(ends) > 0 {
		fmt.Printf("%s\n", value(0))
	}
}

// Writing a callback means knowing the depth and index of the values you want.
// Rather than deriving them from the RESP specification, print the shape of a
// real reply once and read them off: returning true everywhere descends into
// everything, so the walk reports the whole tree. This is the first thing to
// run against an unfamiliar command.
func ExampleRedisResultStream_Walk_printShape() {
	client, err := NewClient(ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Long runs of similar elements are elided so a large reply still prints a
	// readable outline, while short aggregates are shown in full: siblings of a
	// big array repeat, siblings of a three element reply do not.
	const elideOver, showFirst = 4, 2
	size := make(map[int]int64) // element count of the aggregate holding depth d

	// Shape printing is shape-agnostic, so the same callback serves both
	// protocols; passing it as the resp2 argument says so.
	shape := func(i WalkInfo) (bool, error) {
		if i.IsAggregate() {
			size[i.Depth+1] = i.IntVal
		}
		if size[i.Depth] > elideOver && i.Index >= showFirst {
			if i.Index == showFirst {
				fmt.Printf("%s... %d more\n", strings.Repeat("  ", i.Depth), size[i.Depth]-showFirst)
			}
			return false, nil // skip the rest of a long run entirely
		}
		pad := strings.Repeat("  ", i.Depth)
		switch {
		case i.IsAggregate():
			fmt.Printf("%sdepth %d index %d  %c  %d elements\n", pad, i.Depth, i.Index, i.Type, i.IntVal)
		case i.IsString():
			fmt.Printf("%sdepth %d index %d  %c  %q\n", pad, i.Depth, i.Index, i.Type, i.Peek)
		case i.IsInt():
			fmt.Printf("%sdepth %d index %d  %c  %d\n", pad, i.Depth, i.Index, i.Type, i.IntVal)
		default:
			fmt.Printf("%sdepth %d index %d  %c\n", pad, i.Depth, i.Index, i.Type)
		}
		return true, nil
	}
	s := client.DoStream(ctx, client.B().Xrange().Key("mystream").Start("-").End("+").Build())
	if err := s.Walk(shape, shape); err != nil {
		panic(err)
	}

	// For a stream of ten entries this prints:
	//
	//   depth 0 index 0  *  10 elements
	//     depth 1 index 0  *  2 elements
	//       depth 2 index 0  $  "1700000-0"
	//       depth 2 index 1  *  2 elements
	//         depth 3 index 0  $  "field1"
	//         depth 3 index 1  $  "value0"
	//     depth 1 index 1  *  2 elements
	//       depth 2 index 0  $  "1700001-0"
	//       depth 2 index 1  *  2 elements
	//         depth 3 index 0  $  "field1"
	//         depth 3 index 1  $  "value1"
	//     ... 8 more
	//
	// which is the map the callback above is written against.
}
