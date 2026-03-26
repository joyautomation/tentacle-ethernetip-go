package main

import (
	"encoding/binary"
	"math"
	"time"
)

// MockTag implements TagAccessor backed by an in-memory byte buffer.
type MockTag struct {
	data     []byte
	readErr  error
	writeErr error
	written  bool
}

func NewMockTag(size int) *MockTag {
	return &MockTag{data: make([]byte, size)}
}

func NewMockTagFromBytes(data []byte) *MockTag {
	return &MockTag{data: append([]byte{}, data...)}
}

func (m *MockTag) Size() int                    { return len(m.data) }
func (m *MockTag) Read(_ time.Duration) error   { return m.readErr }
func (m *MockTag) Write(_ time.Duration) error  { m.written = true; return m.writeErr }
func (m *MockTag) Close()                       {}

func (m *MockTag) GetBit(offset int) bool {
	byteIdx := offset / 8
	bitIdx := offset % 8
	if byteIdx >= len(m.data) {
		return false
	}
	return m.data[byteIdx]&(1<<uint(bitIdx)) != 0
}

func (m *MockTag) GetInt8(offset int) int8 {
	if offset >= len(m.data) {
		return 0
	}
	return int8(m.data[offset])
}

func (m *MockTag) GetInt16(offset int) int16 {
	if offset+2 > len(m.data) {
		return 0
	}
	return int16(binary.LittleEndian.Uint16(m.data[offset:]))
}

func (m *MockTag) GetInt32(offset int) int32 {
	if offset+4 > len(m.data) {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(m.data[offset:]))
}

func (m *MockTag) GetInt64(offset int) int64 {
	if offset+8 > len(m.data) {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(m.data[offset:]))
}

func (m *MockTag) GetUint8(offset int) uint8 {
	if offset >= len(m.data) {
		return 0
	}
	return m.data[offset]
}

func (m *MockTag) GetUint16(offset int) uint16 {
	if offset+2 > len(m.data) {
		return 0
	}
	return binary.LittleEndian.Uint16(m.data[offset:])
}

func (m *MockTag) GetUint32(offset int) uint32 {
	if offset+4 > len(m.data) {
		return 0
	}
	return binary.LittleEndian.Uint32(m.data[offset:])
}

func (m *MockTag) GetFloat32(offset int) float32 {
	if offset+4 > len(m.data) {
		return 0
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(m.data[offset:]))
}

func (m *MockTag) GetFloat64(offset int) float64 {
	if offset+8 > len(m.data) {
		return 0
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(m.data[offset:]))
}

func (m *MockTag) GetRawBytes(offset, length int) []byte {
	if offset+length > len(m.data) {
		return nil
	}
	result := make([]byte, length)
	copy(result, m.data[offset:offset+length])
	return result
}

func (m *MockTag) GetString(offset int) string {
	strLen := int(m.GetInt32(offset))
	if strLen <= 0 || strLen > 1000 {
		return ""
	}
	buf := m.GetRawBytes(offset+4, strLen)
	if buf == nil {
		return ""
	}
	return string(buf)
}

func (m *MockTag) SetBit(offset int, val bool) {
	byteIdx := offset / 8
	bitIdx := offset % 8
	if byteIdx >= len(m.data) {
		return
	}
	if val {
		m.data[byteIdx] |= 1 << uint(bitIdx)
	} else {
		m.data[byteIdx] &^= 1 << uint(bitIdx)
	}
}

func (m *MockTag) SetInt32(offset int, val int32) {
	if offset+4 > len(m.data) {
		return
	}
	binary.LittleEndian.PutUint32(m.data[offset:], uint32(val))
}

func (m *MockTag) SetFloat32(offset int, val float32) {
	if offset+4 > len(m.data) {
		return
	}
	binary.LittleEndian.PutUint32(m.data[offset:], math.Float32bits(val))
}

func (m *MockTag) SetFloat64(offset int, val float64) {
	if offset+8 > len(m.data) {
		return
	}
	binary.LittleEndian.PutUint64(m.data[offset:], math.Float64bits(val))
}

// Helpers for constructing test data

func (m *MockTag) PutUint16(offset int, val uint16) {
	if offset+2 <= len(m.data) {
		binary.LittleEndian.PutUint16(m.data[offset:], val)
	}
}

func (m *MockTag) PutUint32(offset int, val uint32) {
	if offset+4 <= len(m.data) {
		binary.LittleEndian.PutUint32(m.data[offset:], val)
	}
}

func (m *MockTag) PutInt32(offset int, val int32) {
	if offset+4 <= len(m.data) {
		binary.LittleEndian.PutUint32(m.data[offset:], uint32(val))
	}
}

func (m *MockTag) PutFloat32(offset int, val float32) {
	if offset+4 <= len(m.data) {
		binary.LittleEndian.PutUint32(m.data[offset:], math.Float32bits(val))
	}
}

func (m *MockTag) PutFloat64(offset int, val float64) {
	if offset+8 <= len(m.data) {
		binary.LittleEndian.PutUint64(m.data[offset:], math.Float64bits(val))
	}
}

func (m *MockTag) PutString(offset int, s string) {
	m.PutInt32(offset, int32(len(s)))
	if offset+4+len(s) <= len(m.data) {
		copy(m.data[offset+4:], []byte(s))
	}
}
