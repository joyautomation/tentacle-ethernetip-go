package main

/*
#cgo LDFLAGS: -lplctag
#include <stdlib.h>
#include <libplctag.h>
*/
import "C"
import (
	"fmt"
	"strings"
	"time"
	"unsafe"
)

const (
	plctagStatusOK      = 0
	plctagStatusPending = 1
)

// PlcTag wraps a libplctag tag handle.
type PlcTag struct {
	handle C.int32_t
}

// createTag creates a new libplctag tag with the given attribute string.
func createTag(attrs string, timeout time.Duration) (*PlcTag, error) {
	cAttrs := C.CString(attrs)
	defer C.free(unsafe.Pointer(cAttrs))

	handle := C.plc_tag_create(cAttrs, C.int(timeout.Milliseconds()))
	if handle < 0 {
		return nil, fmt.Errorf("plc_tag_create failed: %s (rc=%d)", plctagError(int(handle)), handle)
	}
	return &PlcTag{handle: handle}, nil
}

// Close destroys the tag and frees resources.
func (t *PlcTag) Close() {
	if t == nil {
		return
	}
	if t.handle >= 0 {
		C.plc_tag_destroy(t.handle)
		t.handle = -1
	}
}

// Read reads the tag value from the PLC (synchronous).
func (t *PlcTag) Read(timeout time.Duration) error {
	rc := C.plc_tag_read(t.handle, C.int(timeout.Milliseconds()))
	if rc != plctagStatusOK {
		return fmt.Errorf("plc_tag_read failed: %s (rc=%d)", plctagError(int(rc)), rc)
	}
	return nil
}

// Status returns the current status of the tag.
// Returns plctagStatusOK (0) when the tag has valid data.
func (t *PlcTag) Status() int {
	return int(C.plc_tag_status(t.handle))
}

// Write writes the tag value to the PLC.
func (t *PlcTag) Write(timeout time.Duration) error {
	rc := C.plc_tag_write(t.handle, C.int(timeout.Milliseconds()))
	if rc != plctagStatusOK {
		return fmt.Errorf("plc_tag_write failed: %s (rc=%d)", plctagError(int(rc)), rc)
	}
	return nil
}

// Size returns the size of the tag data buffer in bytes.
func (t *PlcTag) Size() int {
	return int(C.plc_tag_get_size(t.handle))
}

// GetUint8 reads a uint8 at the given byte offset.
func (t *PlcTag) GetUint8(offset int) uint8 {
	return uint8(C.plc_tag_get_uint8(t.handle, C.int(offset)))
}

// GetUint16 reads a uint16 at the given byte offset.
func (t *PlcTag) GetUint16(offset int) uint16 {
	return uint16(C.plc_tag_get_uint16(t.handle, C.int(offset)))
}

// GetUint32 reads a uint32 at the given byte offset.
func (t *PlcTag) GetUint32(offset int) uint32 {
	return uint32(C.plc_tag_get_uint32(t.handle, C.int(offset)))
}

// GetInt8 reads an int8 at the given byte offset.
func (t *PlcTag) GetInt8(offset int) int8 {
	return int8(C.plc_tag_get_int8(t.handle, C.int(offset)))
}

// GetInt16 reads an int16 at the given byte offset.
func (t *PlcTag) GetInt16(offset int) int16 {
	return int16(C.plc_tag_get_int16(t.handle, C.int(offset)))
}

// GetInt32 reads an int32 at the given byte offset.
func (t *PlcTag) GetInt32(offset int) int32 {
	return int32(C.plc_tag_get_int32(t.handle, C.int(offset)))
}

// GetInt64 reads an int64 at the given byte offset.
func (t *PlcTag) GetInt64(offset int) int64 {
	return int64(C.plc_tag_get_int64(t.handle, C.int(offset)))
}

// GetFloat32 reads a float32 at the given byte offset.
func (t *PlcTag) GetFloat32(offset int) float32 {
	return float32(C.plc_tag_get_float32(t.handle, C.int(offset)))
}

// GetFloat64 reads a float64 at the given byte offset.
func (t *PlcTag) GetFloat64(offset int) float64 {
	return float64(C.plc_tag_get_float64(t.handle, C.int(offset)))
}

// GetBit reads a single bit at the given bit offset.
func (t *PlcTag) GetBit(offset int) bool {
	return C.plc_tag_get_bit(t.handle, C.int(offset)) != 0
}

// SetFloat32 writes a float32 at the given byte offset.
func (t *PlcTag) SetFloat32(offset int, val float32) {
	C.plc_tag_set_float32(t.handle, C.int(offset), C.float(val))
}

// SetFloat64 writes a float64 at the given byte offset.
func (t *PlcTag) SetFloat64(offset int, val float64) {
	C.plc_tag_set_float64(t.handle, C.int(offset), C.double(val))
}

// SetInt32 writes an int32 at the given byte offset.
func (t *PlcTag) SetInt32(offset int, val int32) {
	C.plc_tag_set_int32(t.handle, C.int(offset), C.int32_t(val))
}

// SetBit writes a single bit at the given bit offset.
func (t *PlcTag) SetBit(offset int, val bool) {
	v := C.int(0)
	if val {
		v = 1
	}
	C.plc_tag_set_bit(t.handle, C.int(offset), v)
}

// GetRawBytes reads raw bytes from the tag buffer.
func (t *PlcTag) GetRawBytes(offset, length int) []byte {
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = t.GetUint8(offset + i)
	}
	return buf
}

// GetString reads a Rockwell-format string (4-byte length prefix + chars) at offset.
func (t *PlcTag) GetString(offset int) string {
	strLen := int(t.GetInt32(offset))
	if strLen <= 0 || strLen > 1000 {
		return ""
	}
	buf := t.GetRawBytes(offset+4, strLen)
	return string(buf)
}

// plctagError converts a libplctag error code to a string.
func plctagError(rc int) string {
	cStr := C.plc_tag_decode_error(C.int(rc))
	return C.GoString(cStr)
}

// buildTagAttrs builds the libplctag attribute string for a tag.
// If autoSyncMs > 0, enables libplctag auto-sync which batches reads
// using CIP Multiple Service Packets internally.
func buildTagAttrs(gateway string, port int, tagName string, autoSyncMs int) string {
	if port == 0 {
		port = 44818
	}
	parts := []string{
		"protocol=ab-eip",
		fmt.Sprintf("gateway=%s", gateway),
		"path=1,0",
		"plc=ControlLogix",
		fmt.Sprintf("gateway_port=%d", port),
		fmt.Sprintf("name=%s", tagName),
	}
	if autoSyncMs > 0 {
		parts = append(parts, fmt.Sprintf("auto_sync_read_ms=%d", autoSyncMs))
	}
	return strings.Join(parts, "&")
}

// buildListTagAttrs builds the attribute string for listing tags (@tags).
func buildListTagAttrs(gateway string, port int) string {
	return buildTagAttrs(gateway, port, "@tags", 0)
}

// buildUdtAttrs builds the attribute string for reading a UDT template.
func buildUdtAttrs(gateway string, port int, templateID uint16) string {
	return buildTagAttrs(gateway, port, fmt.Sprintf("@udt/%d", templateID), 0)
}
