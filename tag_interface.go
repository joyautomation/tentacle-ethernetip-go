package main

import "time"

// TagAccessor abstracts libplctag tag operations for testability.
// *PlcTag satisfies this interface implicitly.
type TagAccessor interface {
	Size() int
	Read(timeout time.Duration) error
	Write(timeout time.Duration) error
	Close()
	GetBit(offset int) bool
	GetInt8(offset int) int8
	GetInt16(offset int) int16
	GetInt32(offset int) int32
	GetInt64(offset int) int64
	GetUint8(offset int) uint8
	GetUint16(offset int) uint16
	GetUint32(offset int) uint32
	GetFloat32(offset int) float32
	GetFloat64(offset int) float64
	GetRawBytes(offset, length int) []byte
	GetString(offset int) string
	SetBit(offset int, val bool)
	SetInt32(offset int, val int32)
	SetFloat32(offset int, val float32)
	SetFloat64(offset int, val float64)
}
