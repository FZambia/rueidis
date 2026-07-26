package rueidis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
)

// ErrWalkChunked is reported when a response contains a streamed aggregate
// or string of unknown length, which the walk cannot handle.
var ErrWalkChunked = errors.New("rueidis: Walk does not support unbounded messages")

// ErrWalkUnsupportedType is reported when a response contains a RESP type the
// walk does not know. It is wrapped with the offending type byte, so test for it
// with errors.Is rather than by comparison.
var ErrWalkUnsupportedType = errors.New("rueidis: unsupported redis response type for Walk")

// ErrStreamConsumed is returned by Walk when the stream has no response left to
// read, which usually means it has already been walked.
var ErrStreamConsumed = errors.New("rueidis: the stream has already been consumed")

// ErrWalkRESP3Required is returned by Walk on a RESP2 connection when no
// resp2 callback was provided. The reply is drained, nothing is invoked and
// the connection stays usable. Pass a resp2 callback to support RESP2, the
// same function again when the command's reply shape is identical under both
// protocols, or use Do and its As helpers, which normalise shapes per
// command.
var ErrWalkRESP3Required = errors.New("rueidis: Walk requires a resp2 callback on a RESP2 connection")

// WalkFunc is called once per message of a response.
//
// Returning true for an aggregate descends into its elements; returning false
// discards them, and they are consumed off the wire without being parsed. The
// return is ignored for anything that is not an aggregate.
//
// Returning an error stops the reporting but not the reading: the rest of the
// response is still drained so the connection stays usable, and the error is
// then returned by Walk.
type WalkFunc func(WalkInfo) (bool, error)

// WalkInfo describes the RESP message currently being visited.
type WalkInfo struct {
	// Peek holds the complete payload of a string message. It is only valid for
	// the duration of the callback; copy it, with string(Peek) or equivalent,
	// if it must outlive the call. Retaining it tends to appear to work, because the read buffer is
	// only overwritten once it refills, so the mistake surfaces as an occasional
	// wrong value rather than a failure. Holding a field name across the call
	// that delivers its value is the usual shape of it; decide about the name
	// while you hold it, or copy it with String.
	//
	// Values that fit the connection's read buffer, which is all of them under
	// normal configuration, are returned without copying. A value too large to
	// borrow is copied instead, so Peek is never a partial view.
	Peek []byte

	// IntVal is the value of an integer or boolean message, the byte length of
	// a string message, or the declared element count of an aggregate. For maps
	// it is the number of pairs, so twice as many messages follow.
	IntVal int64

	// Type is the RESP type byte of the message.
	Type byte

	// Depth is the nesting level. The response itself is visited at depth 0 and
	// its elements at depth 1.
	Depth int

	// Index is the position of this message within its parent aggregate. Map
	// keys and values are counted separately, so a key is always even.
	Index int
}

// IsAggregate reports whether the message is an array, set or map, and so has
// elements that returning true will descend into. Prefer this to comparing
// Type when any aggregate should be entered, since sets and maps are distinct
// types on the wire.
func (i WalkInfo) IsAggregate() bool {
	return i.Type == typeArray || i.Type == typeSet || i.Type == typeMap
}

// IsString reports whether the message carries a string payload in Peek.
// Doubles and big numbers are included: RESP transmits them as text, and Peek
// holds that text. A verbatim string keeps its format prefix such as "txt:",
// the same way Do keeps it.
func (i WalkInfo) IsString() bool {
	return i.Type == typeBlobString || i.Type == typeVerbatimString ||
		i.Type == typeSimpleString || i.Type == typeBigNumber || i.Type == typeFloat
}

// IsInt reports whether the message is an integer or boolean, whose value is in
// IntVal.
func (i WalkInfo) IsInt() bool {
	return i.Type == typeInteger || i.Type == typeBool
}

// IsNull reports whether the message is a null, in either RESP2 or RESP3 form.
func (i WalkInfo) IsNull() bool {
	return i.Type == typeNull
}

// IsError reports whether the message is an error element inside an aggregate,
// with its text in Peek. An error at the top level never reaches the callback:
// it is returned from Walk as a *RedisError instead.
func (i WalkInfo) IsError() bool {
	return i.Type == typeSimpleErr || i.Type == typeBlobErr
}

// Walk reads the next response directly off the connection and reports every
// RESP message in it to fn, one call per message. It exists for hot paths
// where Do costs too much: Do parses the whole response into a RedisMessage
// tree before the caller can decide what to keep, so a large reply is fully
// allocated even when only a fraction of it is wanted. Walk allocates only a
// small constant amount itself, however large the response; values are read
// from WalkInfo.Peek during the callback, and only what the callback chooses
// to copy costs anything. Do remains the right call everywhere that is not a
// measured hot path.
//
// Returning true for an aggregate descends into its elements. Returning false
// discards them, consumed off the wire without being parsed, which is where
// the savings come from. The response is always consumed in full whatever fn
// returns, so the connection is left aligned and safe to recycle; an error
// from fn stops the reporting, lets the drain finish, and is then returned.
//
// The resp2 callback decides what happens when the connection negotiated
// RESP2, where some commands reshape their replies relative to RESP3, XREAD
// for one nests each stream in an extra array, so a callback keyed on Depth
// and Index can be wrong by a level there:
//
//	Walk(fn, nil)   fn is RESP3 only; ErrWalkRESP3Required on RESP2
//	Walk(fn, fn)    the shape is the same under both protocols
//	Walk(fn3, fn2)  the command reshapes; fn2 is the RESP2 parser
//
// A null response is reported as Nil and an error response as a *RedisError,
// in both cases without any callback running; nulls and errors never reshape,
// so they come back the same under either protocol. Walking a stream that has
// already been read returns ErrStreamConsumed.
func (s *RedisResultStream) Walk(fn WalkFunc, resp2 WalkFunc) error {
	// Distinguish a stream that failed from one that is simply used up, so a
	// second Walk reports rather than quietly doing nothing.
	if s.e != nil && s.e != io.EOF {
		return s.e
	}
	if s.n <= 0 {
		return ErrStreamConsumed
	}
	return s.doWalk(fn, resp2)
}

// doWalk drives the walk and releases the connection.
func (s *RedisResultStream) doWalk(fn WalkFunc, resp2 WalkFunc) error {
	if s.e != nil || s.n <= 0 {
		return s.e
	}
	// Version is 5 exactly when the connection negotiated RESP2; the caller
	// decided with the resp2 argument what happens then. A nil resp2 drains
	// the reply without reporting and the refusal is recorded below.
	visit := true
	if s.w.Version() < 6 {
		if fn = resp2; fn == nil {
			visit = false
		}
	}
	it := walker{r: s.w.r, fn: fn}
	// Runs to completion even after fn returns an error, so that the reply is
	// drained rather than left half read.
	err := it.walk(0, 0, visit)
	// Only a transport error leaves the connection unusable. A Redis error
	// reply, Nil included, is an ordinary reply that was read in full, and an
	// error from fn still let the drain finish, so neither is a reason to throw
	// the connection away.
	fatal := false
	if err != nil {
		if _, ok := err.(*RedisError); !ok {
			fatal = true
		}
	}
	if err == nil {
		err = it.err
	}
	if !visit && !fatal {
		// The RESP2 refusal overrides whatever the drained reply was, nil,
		// null and error replies alike: the contract is one clear error, not
		// a reply dependent mixture.
		err = ErrWalkRESP3Required
	}
	// Only a fatal error truncates the stream. A redis error reply, a null and
	// an error from fn all leave the connection aligned on the next response,
	// so a multi command stream must keep the count it has left: recording them
	// in s.e would release the connection with responses still unread, and the
	// next command to borrow it would read one of those instead of its own.
	if fatal {
		s.e = err
		s.n = 1
	}
	if s.n--; s.n == 0 {
		atomic.AddInt32(&s.w.blcksig, -1)
		s.w.decrWaits()
		if s.e == nil {
			s.e = io.EOF
		} else if fatal {
			s.w.Close()
		}
		s.p.Store(s.w)
	}
	return err
}

type walker struct {
	r   *bufio.Reader
	fn  WalkFunc
	err error // returned by fn; reporting stops but draining continues
}

// halted reports whether reporting should stop because fn returned an error.
// Draining continues either way so that the connection is left aligned.
func (it *walker) halted() bool { return it.err != nil }

// readType reads the next message's type byte, first consuming any push or
// attribute messages ahead of it. Attributes may decorate any message and
// pushes arrive out of band when client side caching runs in BCAST or OPTOUT
// mode, which is why syncRead and streamTo skip them on this same path.
// Neither is a value in its own right, so neither is reported to fn.
func (it *walker) readType() (byte, error) {
	for {
		typ, err := it.r.ReadByte()
		if err != nil {
			return 0, err
		}
		switch typ {
		case typePush:
			if _, err = readArray(it.r); err != nil {
				return 0, err
			}
		case typeAttribute:
			if _, err = readMap(it.r); err != nil {
				return 0, err
			}
		default:
			return typ, nil
		}
	}
}

// walk consumes exactly one message. When visit is false the message and its
// children are consumed without being reported, which is how a subtree that fn
// declined gets discarded.
func (it *walker) walk(depth, index int, visit bool) error {
	typ, err := it.readType()
	if err != nil {
		return err
	}

	switch typ {
	case typeArray, typeSet, typeMap:
		n, err := readI(it.r)
		if err != nil {
			if err == errChunked {
				return ErrWalkChunked
			}
			return err
		}
		if n < 0 { // RESP2 null array
			if depth == 0 {
				return Nil
			}
			// Reported identically to a RESP3 null: the encoding difference is
			// the protocol's business, not the callback's.
			it.report(WalkInfo{Type: typeNull, Depth: depth, Index: index}, visit)
			return nil
		}
		count := n
		if typ == typeMap {
			count *= 2
		}
		descend := it.report(WalkInfo{Type: typ, IntVal: n, Depth: depth, Index: index}, visit)
		for i := int64(0); i < count; i++ {
			// Children are always consumed; only reporting is suppressed, so
			// the connection stays aligned however fn behaves.
			if err := it.walk(depth+1, int(i), visit && descend && !it.halted()); err != nil {
				return err
			}
		}
		return nil

	case typeBlobString, typeVerbatimString, typeBlobErr:
		n, err := readI(it.r)
		if err != nil {
			if err == errChunked {
				return ErrWalkChunked
			}
			return err
		}
		if n < 0 { // RESP2 null bulk
			if depth == 0 {
				return Nil
			}
			// Reported identically to a RESP3 null: the encoding difference is
			// the protocol's business, not the callback's.
			it.report(WalkInfo{Type: typeNull, Depth: depth, Index: index}, visit)
			return nil
		}
		if depth == 0 && typ == typeBlobErr {
			return it.readErrorAt(int(n))
		}
		info := WalkInfo{Type: typ, IntVal: n, Depth: depth, Index: index}
		total := int(n) + 2
		if !visit { // declined subtree: consume without materialising anything
			if _, err := it.r.Discard(total); err != nil {
				return err
			}
			return nil
		}
		if total <= it.r.Size() {
			// Peek payload and CRLF together: peeking only the payload lets the
			// following Discard refill and slide the buffer, invalidating it.
			b, err := it.r.Peek(total)
			if err != nil {
				return err
			}
			info.Peek = b[:n]
			it.report(info, visit)
			if _, err := it.r.Discard(total); err != nil {
				return err
			}
			return nil
		}
		// Larger than the read buffer, so it cannot be borrowed. Copy it rather
		// than hand back a short view, so Peek is always the complete value.
		buf := make([]byte, n)
		if _, err := io.ReadFull(it.r, buf); err != nil {
			return err
		}
		if _, err := it.r.Discard(2); err != nil {
			return err
		}
		info.Peek = buf
		it.report(info, visit)
		return nil

	case typeSimpleString, typeFloat, typeBigNumber, typeSimpleErr:
		line, err := it.r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// Longer than the read buffer, so it cannot be borrowed. Copy it
			// rather than fail, matching readS. Redis keeps these short, but a
			// small ReadBufferEachConn or a long error message reaches here.
			//
			// The copy has to happen before ReadBytes: what ReadSlice returned
			// aliases the read buffer, and refilling it overwrites the bytes.
			head := make([]byte, len(line))
			copy(head, line)
			var rest []byte
			if rest, err = it.r.ReadBytes('\n'); err == nil {
				head = append(head, rest...)
				line = head
			}
		}
		if err != nil {
			return err
		}
		if len(line) < 2 {
			return errors.New(unexpectedNoCRLF)
		}
		payload := line[:len(line)-2]
		if depth == 0 && typ == typeSimpleErr {
			m := strmsg(typeSimpleErr, string(payload))
			return m.Error()
		}
		info := WalkInfo{
			Type: typ, IntVal: int64(len(payload)), Depth: depth, Index: index,
		}
		if visit {
			info.Peek = payload
		}
		it.report(info, visit)
		return nil

	case typeInteger, typeBool:
		var v int64
		if typ == typeBool {
			b, err := it.r.ReadByte()
			if err != nil {
				return err
			}
			if b == 't' {
				v = 1
			}
			if _, err := it.r.Discard(2); err != nil {
				return err
			}
		} else if v, err = readI(it.r); err != nil {
			return err
		}
		it.report(WalkInfo{Type: typ, IntVal: v, Depth: depth, Index: index}, visit)
		return nil

	case typeNull, typeEnd:
		if _, err := it.r.Discard(2); err != nil {
			return err
		}
		if depth == 0 {
			return Nil
		}
		it.report(WalkInfo{Type: typeNull, Depth: depth, Index: index}, visit)
		return nil

	default:
		return fmt.Errorf("%w: %q", ErrWalkUnsupportedType, typ)
	}
}

// readErrorAt consumes a top level blob error of the given length and returns
// it, so that error replies surface the same way they do from Do.
func (it *walker) readErrorAt(n int) error {
	buf := make([]byte, n)
	if _, err := io.ReadFull(it.r, buf); err != nil {
		return err
	}
	if _, err := it.r.Discard(2); err != nil {
		return err
	}
	m := strmsg(typeBlobErr, string(buf))
	return m.Error()
}

func (it *walker) report(info WalkInfo, visit bool) bool {
	if !visit || it.halted() {
		return false
	}
	descend, err := it.fn(info)
	if err != nil {
		it.err = err
		return false
	}
	return descend
}
