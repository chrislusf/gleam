package util

// NOTE: THIS FILE WAS PRODUCED BY THE
// TRUEPACK CODE GENERATION TOOL (github.com/glycerine/truepack)
// DO NOT EDIT

import (
	"github.com/glycerine/truepack/msgp"
)

// DecodeMsg implements msgp.Decodable
// We treat empty fields as if we read a Nil from the wire.
func (z *Row) DecodeMsg(dc *msgp.Reader) (err error) {
	var sawTopNil bool
	if dc.IsNil() {
		sawTopNil = true
		err = dc.ReadNil()
		if err != nil {
			return
		}
		dc.PushAlwaysNil()
	}

	var field []byte
	_ = field
	const maxFields2zgensym_56fd93edf47ccbd5_3 = 3

	// -- templateDecodeMsg starts here--
	var totalEncodedFields2zgensym_56fd93edf47ccbd5_3 uint32
	totalEncodedFields2zgensym_56fd93edf47ccbd5_3, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	encodedFieldsLeft2zgensym_56fd93edf47ccbd5_3 := totalEncodedFields2zgensym_56fd93edf47ccbd5_3
	missingFieldsLeft2zgensym_56fd93edf47ccbd5_3 := maxFields2zgensym_56fd93edf47ccbd5_3 - totalEncodedFields2zgensym_56fd93edf47ccbd5_3

	var nextMiss2zgensym_56fd93edf47ccbd5_3 int32 = -1
	var found2zgensym_56fd93edf47ccbd5_3 [maxFields2zgensym_56fd93edf47ccbd5_3]bool
	var curField2zgensym_56fd93edf47ccbd5_3 string

doneWithStruct2zgensym_56fd93edf47ccbd5_3:
	// First fill all the encoded fields, then
	// treat the remaining, missing fields, as Nil.
	for encodedFieldsLeft2zgensym_56fd93edf47ccbd5_3 > 0 || missingFieldsLeft2zgensym_56fd93edf47ccbd5_3 > 0 {
		//fmt.Printf("encodedFieldsLeft: %v, missingFieldsLeft: %v, found: '%v', fields: '%#v'\n", encodedFieldsLeft2zgensym_56fd93edf47ccbd5_3, missingFieldsLeft2zgensym_56fd93edf47ccbd5_3, msgp.ShowFound(found2zgensym_56fd93edf47ccbd5_3[:]), decodeMsgFieldOrder2zgensym_56fd93edf47ccbd5_3)
		if encodedFieldsLeft2zgensym_56fd93edf47ccbd5_3 > 0 {
			encodedFieldsLeft2zgensym_56fd93edf47ccbd5_3--
			field, err = dc.ReadMapKeyPtr()
			if err != nil {
				return
			}
			curField2zgensym_56fd93edf47ccbd5_3 = msgp.UnsafeString(field)
		} else {
			//missing fields need handling
			if nextMiss2zgensym_56fd93edf47ccbd5_3 < 0 {
				// tell the reader to only give us Nils
				// until further notice.
				dc.PushAlwaysNil()
				nextMiss2zgensym_56fd93edf47ccbd5_3 = 0
			}
			for nextMiss2zgensym_56fd93edf47ccbd5_3 < maxFields2zgensym_56fd93edf47ccbd5_3 && (found2zgensym_56fd93edf47ccbd5_3[nextMiss2zgensym_56fd93edf47ccbd5_3] || decodeMsgFieldSkip2zgensym_56fd93edf47ccbd5_3[nextMiss2zgensym_56fd93edf47ccbd5_3]) {
				nextMiss2zgensym_56fd93edf47ccbd5_3++
			}
			if nextMiss2zgensym_56fd93edf47ccbd5_3 == maxFields2zgensym_56fd93edf47ccbd5_3 {
				// filled all the empty fields!
				break doneWithStruct2zgensym_56fd93edf47ccbd5_3
			}
			missingFieldsLeft2zgensym_56fd93edf47ccbd5_3--
			curField2zgensym_56fd93edf47ccbd5_3 = decodeMsgFieldOrder2zgensym_56fd93edf47ccbd5_3[nextMiss2zgensym_56fd93edf47ccbd5_3]
		}
		//fmt.Printf("switching on curField: '%v'\n", curField2zgensym_56fd93edf47ccbd5_3)
		switch curField2zgensym_56fd93edf47ccbd5_3 {
		// -- templateDecodeMsg ends here --

		case "K__slc":
			found2zgensym_56fd93edf47ccbd5_3[0] = true
			var zgensym_56fd93edf47ccbd5_4 uint32
			zgensym_56fd93edf47ccbd5_4, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.K) >= int(zgensym_56fd93edf47ccbd5_4) {
				z.K = (z.K)[:zgensym_56fd93edf47ccbd5_4]
			} else {
				z.K = make([]interface{}, zgensym_56fd93edf47ccbd5_4)
			}
			for zgensym_56fd93edf47ccbd5_0 := range z.K {
				z.K[zgensym_56fd93edf47ccbd5_0], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		case "V__slc":
			found2zgensym_56fd93edf47ccbd5_3[1] = true
			var zgensym_56fd93edf47ccbd5_5 uint32
			zgensym_56fd93edf47ccbd5_5, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.V) >= int(zgensym_56fd93edf47ccbd5_5) {
				z.V = (z.V)[:zgensym_56fd93edf47ccbd5_5]
			} else {
				z.V = make([]interface{}, zgensym_56fd93edf47ccbd5_5)
			}
			for zgensym_56fd93edf47ccbd5_1 := range z.V {
				z.V[zgensym_56fd93edf47ccbd5_1], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		case "T__i64":
			found2zgensym_56fd93edf47ccbd5_3[2] = true
			z.T, err = dc.ReadInt64()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	if nextMiss2zgensym_56fd93edf47ccbd5_3 != -1 {
		dc.PopAlwaysNil()
	}

	if sawTopNil {
		dc.PopAlwaysNil()
	}

	if p, ok := interface{}(z).(msgp.PostLoad); ok {
		p.PostLoadHook()
	}

	return
}

// fields of Row
var decodeMsgFieldOrder2zgensym_56fd93edf47ccbd5_3 = []string{"K__slc", "V__slc", "T__i64"}

var decodeMsgFieldSkip2zgensym_56fd93edf47ccbd5_3 = []bool{false, false, false}

// fieldsNotEmpty supports omitempty tags
func (z *Row) fieldsNotEmpty(isempty []bool) uint32 {
	if len(isempty) == 0 {
		return 3
	}
	var fieldsInUse uint32 = 3
	isempty[0] = (len(z.K) == 0) // string, omitempty
	if isempty[0] {
		fieldsInUse--
	}
	isempty[1] = (len(z.V) == 0) // string, omitempty
	if isempty[1] {
		fieldsInUse--
	}
	isempty[2] = (z.T == 0) // number, omitempty
	if isempty[2] {
		fieldsInUse--
	}

	return fieldsInUse
}

// EncodeMsg implements msgp.Encodable
func (z *Row) EncodeMsg(en *msgp.Writer) (err error) {
	if p, ok := interface{}(z).(msgp.PreSave); ok {
		p.PreSaveHook()
	}

	// honor the omitempty tags
	var empty_zgensym_56fd93edf47ccbd5_6 [3]bool
	fieldsInUse_zgensym_56fd93edf47ccbd5_7 := z.fieldsNotEmpty(empty_zgensym_56fd93edf47ccbd5_6[:])

	// map header
	err = en.WriteMapHeader(fieldsInUse_zgensym_56fd93edf47ccbd5_7)
	if err != nil {
		return err
	}

	if !empty_zgensym_56fd93edf47ccbd5_6[0] {
		// write "K__slc"
		err = en.Append(0xa6, 0x4b, 0x5f, 0x5f, 0x73, 0x6c, 0x63)
		if err != nil {
			return err
		}
		err = en.WriteArrayHeader(uint32(len(z.K)))
		if err != nil {
			return
		}
		for zgensym_56fd93edf47ccbd5_0 := range z.K {
			err = en.WriteIntf(z.K[zgensym_56fd93edf47ccbd5_0])
			if err != nil {
				return
			}
		}
	}

	if !empty_zgensym_56fd93edf47ccbd5_6[1] {
		// write "V__slc"
		err = en.Append(0xa6, 0x56, 0x5f, 0x5f, 0x73, 0x6c, 0x63)
		if err != nil {
			return err
		}
		err = en.WriteArrayHeader(uint32(len(z.V)))
		if err != nil {
			return
		}
		for zgensym_56fd93edf47ccbd5_1 := range z.V {
			err = en.WriteIntf(z.V[zgensym_56fd93edf47ccbd5_1])
			if err != nil {
				return
			}
		}
	}

	if !empty_zgensym_56fd93edf47ccbd5_6[2] {
		// write "T__i64"
		err = en.Append(0xa6, 0x54, 0x5f, 0x5f, 0x69, 0x36, 0x34)
		if err != nil {
			return err
		}
		err = en.WriteInt64(z.T)
		if err != nil {
			return
		}
	}

	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Row) MarshalMsg(b []byte) (o []byte, err error) {
	if p, ok := interface{}(z).(msgp.PreSave); ok {
		p.PreSaveHook()
	}

	o = msgp.Require(b, z.Msgsize())

	// honor the omitempty tags
	var empty [3]bool
	fieldsInUse := z.fieldsNotEmpty(empty[:])
	o = msgp.AppendMapHeader(o, fieldsInUse)

	if !empty[0] {
		// string "K__slc"
		o = append(o, 0xa6, 0x4b, 0x5f, 0x5f, 0x73, 0x6c, 0x63)
		o = msgp.AppendArrayHeader(o, uint32(len(z.K)))
		for zgensym_56fd93edf47ccbd5_0 := range z.K {
			o, err = msgp.AppendIntf(o, z.K[zgensym_56fd93edf47ccbd5_0])
			if err != nil {
				return
			}
		}
	}

	if !empty[1] {
		// string "V__slc"
		o = append(o, 0xa6, 0x56, 0x5f, 0x5f, 0x73, 0x6c, 0x63)
		o = msgp.AppendArrayHeader(o, uint32(len(z.V)))
		for zgensym_56fd93edf47ccbd5_1 := range z.V {
			o, err = msgp.AppendIntf(o, z.V[zgensym_56fd93edf47ccbd5_1])
			if err != nil {
				return
			}
		}
	}

	if !empty[2] {
		// string "T__i64"
		o = append(o, 0xa6, 0x54, 0x5f, 0x5f, 0x69, 0x36, 0x34)
		o = msgp.AppendInt64(o, z.T)
	}

	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Row) UnmarshalMsg(bts []byte) (o []byte, err error) {
	cfg := &msgp.RuntimeConfig{UnsafeZeroCopy: true}
	return z.UnmarshalMsgWithCfg(bts, cfg)
}
func (z *Row) UnmarshalMsgWithCfg(bts []byte, cfg *msgp.RuntimeConfig) (o []byte, err error) {
	var nbs msgp.NilBitsStack
	nbs.Init(cfg)
	var sawTopNil bool
	if msgp.IsNil(bts) {
		sawTopNil = true
		bts = nbs.PushAlwaysNil(bts[1:])
	}

	var field []byte
	_ = field
	const maxFields8zgensym_56fd93edf47ccbd5_9 = 3

	// -- templateUnmarshalMsg starts here--
	var totalEncodedFields8zgensym_56fd93edf47ccbd5_9 uint32
	if !nbs.AlwaysNil {
		totalEncodedFields8zgensym_56fd93edf47ccbd5_9, bts, err = nbs.ReadMapHeaderBytes(bts)
		if err != nil {
			return
		}
	}
	encodedFieldsLeft8zgensym_56fd93edf47ccbd5_9 := totalEncodedFields8zgensym_56fd93edf47ccbd5_9
	missingFieldsLeft8zgensym_56fd93edf47ccbd5_9 := maxFields8zgensym_56fd93edf47ccbd5_9 - totalEncodedFields8zgensym_56fd93edf47ccbd5_9

	var nextMiss8zgensym_56fd93edf47ccbd5_9 int32 = -1
	var found8zgensym_56fd93edf47ccbd5_9 [maxFields8zgensym_56fd93edf47ccbd5_9]bool
	var curField8zgensym_56fd93edf47ccbd5_9 string

doneWithStruct8zgensym_56fd93edf47ccbd5_9:
	// First fill all the encoded fields, then
	// treat the remaining, missing fields, as Nil.
	for encodedFieldsLeft8zgensym_56fd93edf47ccbd5_9 > 0 || missingFieldsLeft8zgensym_56fd93edf47ccbd5_9 > 0 {
		//fmt.Printf("encodedFieldsLeft: %v, missingFieldsLeft: %v, found: '%v', fields: '%#v'\n", encodedFieldsLeft8zgensym_56fd93edf47ccbd5_9, missingFieldsLeft8zgensym_56fd93edf47ccbd5_9, msgp.ShowFound(found8zgensym_56fd93edf47ccbd5_9[:]), unmarshalMsgFieldOrder8zgensym_56fd93edf47ccbd5_9)
		if encodedFieldsLeft8zgensym_56fd93edf47ccbd5_9 > 0 {
			encodedFieldsLeft8zgensym_56fd93edf47ccbd5_9--
			field, bts, err = nbs.ReadMapKeyZC(bts)
			if err != nil {
				return
			}
			curField8zgensym_56fd93edf47ccbd5_9 = msgp.UnsafeString(field)
		} else {
			//missing fields need handling
			if nextMiss8zgensym_56fd93edf47ccbd5_9 < 0 {
				// set bts to contain just mnil (0xc0)
				bts = nbs.PushAlwaysNil(bts)
				nextMiss8zgensym_56fd93edf47ccbd5_9 = 0
			}
			for nextMiss8zgensym_56fd93edf47ccbd5_9 < maxFields8zgensym_56fd93edf47ccbd5_9 && (found8zgensym_56fd93edf47ccbd5_9[nextMiss8zgensym_56fd93edf47ccbd5_9] || unmarshalMsgFieldSkip8zgensym_56fd93edf47ccbd5_9[nextMiss8zgensym_56fd93edf47ccbd5_9]) {
				nextMiss8zgensym_56fd93edf47ccbd5_9++
			}
			if nextMiss8zgensym_56fd93edf47ccbd5_9 == maxFields8zgensym_56fd93edf47ccbd5_9 {
				// filled all the empty fields!
				break doneWithStruct8zgensym_56fd93edf47ccbd5_9
			}
			missingFieldsLeft8zgensym_56fd93edf47ccbd5_9--
			curField8zgensym_56fd93edf47ccbd5_9 = unmarshalMsgFieldOrder8zgensym_56fd93edf47ccbd5_9[nextMiss8zgensym_56fd93edf47ccbd5_9]
		}
		//fmt.Printf("switching on curField: '%v'\n", curField8zgensym_56fd93edf47ccbd5_9)
		switch curField8zgensym_56fd93edf47ccbd5_9 {
		// -- templateUnmarshalMsg ends here --

		case "K__slc":
			found8zgensym_56fd93edf47ccbd5_9[0] = true
			if nbs.AlwaysNil {
				(z.K) = (z.K)[:0]
			} else {

				var zgensym_56fd93edf47ccbd5_10 uint32
				zgensym_56fd93edf47ccbd5_10, bts, err = nbs.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(z.K) >= int(zgensym_56fd93edf47ccbd5_10) {
					z.K = (z.K)[:zgensym_56fd93edf47ccbd5_10]
				} else {
					z.K = make([]interface{}, zgensym_56fd93edf47ccbd5_10)
				}
				for zgensym_56fd93edf47ccbd5_0 := range z.K {
					z.K[zgensym_56fd93edf47ccbd5_0], bts, err = nbs.ReadIntfBytes(bts)

					if err != nil {
						return
					}
				}
			}
		case "V__slc":
			found8zgensym_56fd93edf47ccbd5_9[1] = true
			if nbs.AlwaysNil {
				(z.V) = (z.V)[:0]
			} else {

				var zgensym_56fd93edf47ccbd5_11 uint32
				zgensym_56fd93edf47ccbd5_11, bts, err = nbs.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(z.V) >= int(zgensym_56fd93edf47ccbd5_11) {
					z.V = (z.V)[:zgensym_56fd93edf47ccbd5_11]
				} else {
					z.V = make([]interface{}, zgensym_56fd93edf47ccbd5_11)
				}
				for zgensym_56fd93edf47ccbd5_1 := range z.V {
					z.V[zgensym_56fd93edf47ccbd5_1], bts, err = nbs.ReadIntfBytes(bts)

					if err != nil {
						return
					}
				}
			}
		case "T__i64":
			found8zgensym_56fd93edf47ccbd5_9[2] = true
			z.T, bts, err = nbs.ReadInt64Bytes(bts)

			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	if nextMiss8zgensym_56fd93edf47ccbd5_9 != -1 {
		bts = nbs.PopAlwaysNil()
	}

	if sawTopNil {
		bts = nbs.PopAlwaysNil()
	}
	o = bts
	if p, ok := interface{}(z).(msgp.PostLoad); ok {
		p.PostLoadHook()
	}

	return
}

// fields of Row
var unmarshalMsgFieldOrder8zgensym_56fd93edf47ccbd5_9 = []string{"K__slc", "V__slc", "T__i64"}

var unmarshalMsgFieldSkip8zgensym_56fd93edf47ccbd5_9 = []bool{false, false, false}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Row) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for zgensym_56fd93edf47ccbd5_0 := range z.K {
		s += msgp.GuessSize(z.K[zgensym_56fd93edf47ccbd5_0])
	}
	s += 7 + msgp.ArrayHeaderSize
	for zgensym_56fd93edf47ccbd5_1 := range z.V {
		s += msgp.GuessSize(z.V[zgensym_56fd93edf47ccbd5_1])
	}
	s += 7 + msgp.Int64Size
	return
}
