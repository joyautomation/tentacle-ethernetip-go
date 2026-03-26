package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// ═══════════════════════════════════════════════════════════════════════════
// Test helpers
// ═══════════════════════════════════════════════════════════════════════════

func startTestNATS(t *testing.T) (*nats.Conn, func()) {
	t.Helper()
	opts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	return nc, func() {
		nc.Close()
		s.Shutdown()
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// shouldPublish (RBE filtering)
// ═══════════════════════════════════════════════════════════════════════════

func TestShouldPublish(t *testing.T) {
	tests := []struct {
		name   string
		cached *CachedVar
		newVal interface{}
		nowMs  int64
		want   bool
	}{
		{
			"DisableRBE always publishes",
			&CachedVar{DisableRBE: true, LastPublishedTime: 1000, LastPublishedValue: 42.0},
			42.0, 1001, true,
		},
		{
			"first read always publishes",
			&CachedVar{LastPublishedTime: 0},
			42.0, 1000, true,
		},
		{
			"no deadband - value changed",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: 41.0},
			42.0, 2000, true,
		},
		{
			"no deadband - value unchanged",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: 42.0},
			42.0, 2000, false,
		},
		{
			"deadband - change below threshold suppressed",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 1.0},
			},
			42.5, 2000, false,
		},
		{
			"deadband - change above threshold publishes",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 1.0},
			},
			43.5, 2000, true,
		},
		{
			"deadband - exactly at threshold suppressed",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 1.0},
			},
			43.0, 2000, false, // math.Abs(43-42) = 1.0, NOT > 1.0
		},
		{
			"minTime not elapsed - suppress even with large change",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 0.1, MinTime: 5000},
			},
			99.0, 2000, false,
		},
		{
			"minTime elapsed - publish",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 0.1, MinTime: 500},
			},
			99.0, 2000, true,
		},
		{
			"maxTime exceeded - force publish even with no change",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 100.0, MaxTime: 5000},
			},
			42.0, 6001, true,
		},
		{
			"maxTime not yet exceeded",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: 42.0,
				Deadband: &DeadBandConfig{Value: 100.0, MaxTime: 5000},
			},
			42.0, 4000, false,
		},
		{
			"boolean change with no deadband",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: true},
			false, 2000, true,
		},
		{
			"boolean unchanged with no deadband",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: true},
			true, 2000, false,
		},
		{
			"string change with no deadband",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: "hello"},
			"world", 2000, true,
		},
		{
			"string unchanged with no deadband",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: "hello"},
			"hello", 2000, false,
		},
		{
			"boolean change with deadband - numeric comparison",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: true,
				Deadband: &DeadBandConfig{Value: 0.5},
			},
			false, 2000, true, // |0-1| = 1.0 > 0.5
		},
		{
			"boolean change with large deadband - suppressed",
			&CachedVar{
				LastPublishedTime: 1000, LastPublishedValue: true,
				Deadband: &DeadBandConfig{Value: 2.0},
			},
			false, 2000, false, // |0-1| = 1.0 NOT > 2.0
		},
		{
			"nil previous value with no deadband",
			&CachedVar{LastPublishedTime: 1000, LastPublishedValue: nil},
			42.0, 2000, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPublish(tt.cached, tt.newVal, tt.nowMs)
			if got != tt.want {
				t.Errorf("shouldPublish() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// valuesEqual
// ═══════════════════════════════════════════════════════════════════════════

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"nil nil", nil, nil, true},
		{"nil vs value", nil, 42, false},
		{"value vs nil", 42, nil, false},
		{"same int", 42, 42, true},
		{"different int", 42, 43, false},
		{"int vs float64", int(42), float64(42), true},
		{"int32 vs int64", int32(42), int64(42), true},
		{"float32 vs float64", float32(2.5), float64(2.5), true},
		{"same string", "hello", "hello", true},
		{"different string", "hello", "world", false},
		{"same bool", true, true, true},
		{"different bool", true, false, false},
		{"bool true vs int 1", true, int(1), true},
		{"bool false vs int 0", false, int(0), true},
		{"uint8 vs int64", uint8(42), int64(42), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// toFloat64
// ═══════════════════════════════════════════════════════════════════════════

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  float64
		ok    bool
	}{
		{"float64", float64(3.14), 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", int(42), 42, true},
		{"int8", int8(-8), -8, true},
		{"int16", int16(16), 16, true},
		{"int32", int32(32), 32, true},
		{"int64", int64(64), 64, true},
		{"uint", uint(10), 10, true},
		{"uint8", uint8(8), 8, true},
		{"uint16", uint16(16), 16, true},
		{"uint32", uint32(32), 32, true},
		{"uint64", uint64(64), 64, true},
		{"bool true", true, 1, true},
		{"bool false", false, 0, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.input)
			if ok != tt.ok {
				t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// readBySize (with MockTag)
// ═══════════════════════════════════════════════════════════════════════════

func TestReadBySize(t *testing.T) {
	t.Run("size 1 - BOOL true", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 1
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "BOOL" {
			t.Errorf("cipType = %q, want BOOL", cipType)
		}
		if val != true {
			t.Errorf("val = %v, want true", val)
		}
	})

	t.Run("size 1 - BOOL false", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 0
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "BOOL" {
			t.Errorf("cipType = %q, want BOOL", cipType)
		}
		if val != false {
			t.Errorf("val = %v, want false", val)
		}
	})

	t.Run("size 1 - SINT", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 42
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "SINT" {
			t.Errorf("cipType = %q, want SINT", cipType)
		}
		if val != int64(42) {
			t.Errorf("val = %v, want 42", val)
		}
	})

	t.Run("size 1 - SINT negative", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 0xFE // -2 as int8
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "SINT" {
			t.Errorf("cipType = %q, want SINT", cipType)
		}
		if val != int64(-2) {
			t.Errorf("val = %v, want -2", val)
		}
	})

	t.Run("size 2 - INT", func(t *testing.T) {
		tag := NewMockTag(2)
		tag.PutUint16(0, 1234)
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "INT" {
			t.Errorf("cipType = %q, want INT", cipType)
		}
		if val != int64(1234) {
			t.Errorf("val = %v, want 1234", val)
		}
	})

	t.Run("size 4 - DINT", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.PutInt32(0, 123456)
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "DINT" {
			t.Errorf("cipType = %q, want DINT", cipType)
		}
		if val != int64(123456) {
			t.Errorf("val = %v, want 123456", val)
		}
	})

	t.Run("size 8 - LREAL", func(t *testing.T) {
		tag := NewMockTag(8)
		tag.PutFloat64(0, 3.14159)
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "LREAL" {
			t.Errorf("cipType = %q, want LREAL", cipType)
		}
		if val != 3.14159 {
			t.Errorf("val = %v, want 3.14159", val)
		}
	})

	t.Run("STRING format", func(t *testing.T) {
		tag := NewMockTag(88)
		tag.PutString(0, "Hello")
		val, cipType, err := readBySize(tag)
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "STRING" {
			t.Errorf("cipType = %q, want STRING", cipType)
		}
		if val != "Hello" {
			t.Errorf("val = %q, want Hello", val)
		}
	})

	t.Run("size 3 - unknown error", func(t *testing.T) {
		tag := NewMockTag(3)
		_, _, err := readBySize(tag)
		if err == nil {
			t.Error("expected error for unknown size")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// readByKnownType (with MockTag)
// ═══════════════════════════════════════════════════════════════════════════

func TestReadByKnownType(t *testing.T) {
	t.Run("BOOL true", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 1
		val, cipType, err := readByKnownType(tag, "BOOL")
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "BOOL" {
			t.Errorf("cipType = %q", cipType)
		}
		if val != true {
			t.Errorf("val = %v, want true", val)
		}
	})

	t.Run("BOOL false", func(t *testing.T) {
		tag := NewMockTag(1)
		val, _, _ := readByKnownType(tag, "BOOL")
		if val != false {
			t.Errorf("val = %v, want false", val)
		}
	})

	t.Run("SINT", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 0xFD // -3
		val, cipType, _ := readByKnownType(tag, "SINT")
		if cipType != "SINT" {
			t.Errorf("cipType = %q", cipType)
		}
		if val != int64(-3) {
			t.Errorf("val = %v, want -3", val)
		}
	})

	t.Run("INT", func(t *testing.T) {
		tag := NewMockTag(2)
		tag.PutUint16(0, uint16(500))
		val, _, _ := readByKnownType(tag, "INT")
		if val != int64(500) {
			t.Errorf("val = %v, want 500", val)
		}
	})

	t.Run("DINT", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.PutInt32(0, 100000)
		val, _, _ := readByKnownType(tag, "DINT")
		if val != int64(100000) {
			t.Errorf("val = %v, want 100000", val)
		}
	})

	t.Run("LINT", func(t *testing.T) {
		tag := NewMockTag(8)
		expected := int64(9999999999)
		tag.SetFloat64(0, 0) // zero the buffer
		// Write int64 directly
		tag.data[0] = byte(expected)
		tag.data[1] = byte(expected >> 8)
		tag.data[2] = byte(expected >> 16)
		tag.data[3] = byte(expected >> 24)
		tag.data[4] = byte(expected >> 32)
		tag.data[5] = byte(expected >> 40)
		tag.data[6] = byte(expected >> 48)
		tag.data[7] = byte(expected >> 56)
		val, _, _ := readByKnownType(tag, "LINT")
		if val != expected {
			t.Errorf("val = %v, want %v", val, expected)
		}
	})

	t.Run("USINT", func(t *testing.T) {
		tag := NewMockTag(1)
		tag.data[0] = 200
		val, _, _ := readByKnownType(tag, "USINT")
		if val != int64(200) {
			t.Errorf("val = %v, want 200", val)
		}
	})

	t.Run("UINT", func(t *testing.T) {
		tag := NewMockTag(2)
		tag.PutUint16(0, 50000)
		val, _, _ := readByKnownType(tag, "UINT")
		if val != int64(50000) {
			t.Errorf("val = %v, want 50000", val)
		}
	})

	t.Run("UDINT", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.PutUint32(0, 3000000000)
		val, _, _ := readByKnownType(tag, "UDINT")
		if val != int64(3000000000) {
			t.Errorf("val = %v, want 3000000000", val)
		}
	})

	t.Run("REAL", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.PutFloat32(0, 3.14)
		val, cipType, _ := readByKnownType(tag, "REAL")
		if cipType != "REAL" {
			t.Errorf("cipType = %q", cipType)
		}
		f, ok := val.(float64)
		if !ok {
			t.Fatalf("val type = %T, want float64", val)
		}
		if math.Abs(f-3.14) > 0.01 {
			t.Errorf("val = %v, want ~3.14", f)
		}
	})

	t.Run("LREAL", func(t *testing.T) {
		tag := NewMockTag(8)
		tag.PutFloat64(0, 2.718281828)
		val, _, _ := readByKnownType(tag, "LREAL")
		if val != 2.718281828 {
			t.Errorf("val = %v, want 2.718281828", val)
		}
	})

	t.Run("STRING", func(t *testing.T) {
		tag := NewMockTag(88)
		tag.PutString(0, "TestString")
		val, cipType, _ := readByKnownType(tag, "STRING")
		if cipType != "STRING" {
			t.Errorf("cipType = %q", cipType)
		}
		if val != "TestString" {
			t.Errorf("val = %q, want TestString", val)
		}
	})

	t.Run("unknown type with size 4 fallback", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.PutInt32(0, 42)
		val, cipType, err := readByKnownType(tag, "CUSTOM")
		if err != nil {
			t.Fatal(err)
		}
		if cipType != "CUSTOM" {
			t.Errorf("cipType = %q, want CUSTOM", cipType)
		}
		if val != int64(42) {
			t.Errorf("val = %v, want 42", val)
		}
	})

	t.Run("unsupported type error", func(t *testing.T) {
		tag := NewMockTag(3) // size != 4
		_, _, err := readByKnownType(tag, "TOTALLY_FAKE")
		if err == nil {
			t.Error("expected error")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// writeTagValue (with MockTag)
// ═══════════════════════════════════════════════════════════════════════════

func TestWriteTagValue(t *testing.T) {
	t.Run("BOOL true", func(t *testing.T) {
		tag := NewMockTag(4)
		if err := writeTagValue(tag, "BOOL", "true"); err != nil {
			t.Fatal(err)
		}
		if !tag.written {
			t.Error("Write not called")
		}
		if !tag.GetBit(0) {
			t.Error("BOOL not set to true")
		}
	})

	t.Run("BOOL false", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.data[0] = 0xFF // start with all bits set
		if err := writeTagValue(tag, "BOOL", "0"); err != nil {
			t.Fatal(err)
		}
		if tag.GetBit(0) {
			t.Error("BOOL not set to false")
		}
	})

	t.Run("DINT", func(t *testing.T) {
		tag := NewMockTag(4)
		if err := writeTagValue(tag, "DINT", "42"); err != nil {
			t.Fatal(err)
		}
		if tag.GetInt32(0) != 42 {
			t.Errorf("got %d, want 42", tag.GetInt32(0))
		}
	})

	t.Run("SINT", func(t *testing.T) {
		tag := NewMockTag(4)
		if err := writeTagValue(tag, "SINT", "-5"); err != nil {
			t.Fatal(err)
		}
		// SINT writes via SetInt32 with int8 cast
		got := int8(tag.GetInt32(0))
		if got != -5 {
			t.Errorf("got %d, want -5", got)
		}
	})

	t.Run("INT", func(t *testing.T) {
		tag := NewMockTag(4)
		if err := writeTagValue(tag, "INT", "1000"); err != nil {
			t.Fatal(err)
		}
		got := int16(tag.GetInt32(0))
		if got != 1000 {
			t.Errorf("got %d, want 1000", got)
		}
	})

	t.Run("REAL", func(t *testing.T) {
		tag := NewMockTag(4)
		if err := writeTagValue(tag, "REAL", "3.14"); err != nil {
			t.Fatal(err)
		}
		got := tag.GetFloat32(0)
		if math.Abs(float64(got)-3.14) > 0.01 {
			t.Errorf("got %v, want ~3.14", got)
		}
	})

	t.Run("LREAL", func(t *testing.T) {
		tag := NewMockTag(8)
		if err := writeTagValue(tag, "LREAL", "2.718"); err != nil {
			t.Fatal(err)
		}
		got := tag.GetFloat64(0)
		if math.Abs(got-2.718) > 0.001 {
			t.Errorf("got %v, want ~2.718", got)
		}
	})

	t.Run("unsupported type error", func(t *testing.T) {
		tag := NewMockTag(4)
		err := writeTagValue(tag, "TOTALLY_FAKE", "42")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("pre-read failure", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.readErr = fmt.Errorf("read failed")
		err := writeTagValue(tag, "DINT", "42")
		if err == nil {
			t.Error("expected error from failed pre-read")
		}
	})

	t.Run("write failure", func(t *testing.T) {
		tag := NewMockTag(4)
		tag.writeErr = fmt.Errorf("write failed")
		err := writeTagValue(tag, "DINT", "42")
		if err == nil {
			t.Error("expected error from failed write")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// buildTagAttrs
// ═══════════════════════════════════════════════════════════════════════════

func TestBuildTagAttrs(t *testing.T) {
	t.Run("standard", func(t *testing.T) {
		got := buildTagAttrs("192.168.1.100", 44818, "MyTag", 0)
		want := "protocol=ab-eip&gateway=192.168.1.100&path=1,0&plc=ControlLogix&gateway_port=44818&name=MyTag"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("default port", func(t *testing.T) {
		got := buildTagAttrs("192.168.1.100", 0, "MyTag", 0)
		if !strings.Contains(got, "gateway_port=44818") {
			t.Error("default port not applied")
		}
	})

	t.Run("with auto sync", func(t *testing.T) {
		got := buildTagAttrs("192.168.1.100", 44818, "MyTag", 500)
		if !strings.Contains(got, "auto_sync_read_ms=500") {
			t.Error("auto_sync_read_ms not set")
		}
	})

	t.Run("without auto sync", func(t *testing.T) {
		got := buildTagAttrs("192.168.1.100", 44818, "MyTag", 0)
		if strings.Contains(got, "auto_sync_read_ms") {
			t.Error("auto_sync_read_ms should not be present when 0")
		}
	})
}

func TestBuildListTagAttrs(t *testing.T) {
	got := buildListTagAttrs("10.0.0.1", 44818)
	if !strings.Contains(got, "name=@tags") {
		t.Error("expected name=@tags")
	}
}

func TestBuildUdtAttrs(t *testing.T) {
	got := buildUdtAttrs("10.0.0.1", 44818, 123)
	if !strings.Contains(got, "name=@udt/123") {
		t.Errorf("expected name=@udt/123 in %q", got)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Rate tracking
// ═══════════════════════════════════════════════════════════════════════════

func TestRateTracking(t *testing.T) {
	t.Run("publish rate", func(t *testing.T) {
		s := NewScanner(nil)
		s.recordPublish(100)
		rate := s.PublishRate()
		// 100 events in 10s window = 10/s
		if rate < 9 || rate > 11 {
			t.Errorf("expected ~10 msgs/s, got %f", rate)
		}
	})

	t.Run("poll rate", func(t *testing.T) {
		s := NewScanner(nil)
		s.recordPolls(50)
		rate := s.PollRate()
		// 50 events in 10s window = 5/s
		if rate < 4 || rate > 6 {
			t.Errorf("expected ~5 tags/s, got %f", rate)
		}
	})

	t.Run("zero rate", func(t *testing.T) {
		s := NewScanner(nil)
		if rate := s.PublishRate(); rate != 0 {
			t.Errorf("expected 0, got %f", rate)
		}
		if rate := s.PollRate(); rate != 0 {
			t.Errorf("expected 0, got %f", rate)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Scanner state
// ═══════════════════════════════════════════════════════════════════════════

func TestScannerEnabledState(t *testing.T) {
	s := NewScanner(nil)

	if !s.IsEnabled() {
		t.Error("scanner should be enabled by default")
	}

	s.SetEnabled(false)
	if s.IsEnabled() {
		t.Error("scanner should be disabled")
	}

	s.SetEnabled(true)
	if !s.IsEnabled() {
		t.Error("scanner should be re-enabled")
	}
}

func TestActiveDevicesEmpty(t *testing.T) {
	s := NewScanner(nil)
	devices := s.ActiveDevices()
	if len(devices) != 0 {
		t.Errorf("expected 0 devices, got %d", len(devices))
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// NATS handler tests
// ═══════════════════════════════════════════════════════════════════════════

func TestHandleSubscribe(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	req := SubscribeRequest{
		DeviceID:     "plc1",
		Host:         "192.168.1.100",
		Port:         44818,
		Tags:         []string{"Tag1", "Tag2"},
		CipTypes:     map[string]string{"Tag1": "DINT", "Tag2": "REAL"},
		SubscriberID: "sub1",
		ScanRate:     500,
	}
	data, _ := json.Marshal(req)

	resp, err := nc.Request("ethernetip.subscribe", data, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatal(err)
	}

	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}
	if result["count"].(float64) != 2 {
		t.Errorf("expected count=2, got %v", result["count"])
	}

	// Verify internal state
	scanner.mu.RLock()
	conn, exists := scanner.connections["plc1"]
	scanner.mu.RUnlock()
	if !exists {
		t.Fatal("connection not created")
	}

	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.Variables) != 2 {
		t.Errorf("expected 2 variables, got %d", len(conn.Variables))
	}
	if conn.Variables["Tag1"].CipType != "DINT" {
		t.Errorf("Tag1 cipType = %q, want DINT", conn.Variables["Tag1"].CipType)
	}
	if conn.Variables["Tag2"].CipType != "REAL" {
		t.Errorf("Tag2 cipType = %q, want REAL", conn.Variables["Tag2"].CipType)
	}
	if !conn.Subscribers["sub1"]["Tag1"] || !conn.Subscribers["sub1"]["Tag2"] {
		t.Error("tags not tracked for subscriber")
	}
}

func TestHandleSubscribeWithDeadbands(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	req := SubscribeRequest{
		DeviceID:     "plc2",
		Host:         "192.168.1.200",
		Tags:         []string{"Temp", "Status"},
		Deadbands:    map[string]DeadBandConfig{"Temp": {Value: 0.5, MinTime: 1000}},
		DisableRBE:   map[string]bool{"Status": true},
		SubscriberID: "sub1",
	}
	data, _ := json.Marshal(req)

	resp, err := nc.Request("ethernetip.subscribe", data, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	json.Unmarshal(resp.Data, &result)
	if result["success"] != true {
		t.Fatalf("subscribe failed: %v", result)
	}

	scanner.mu.RLock()
	conn := scanner.connections["plc2"]
	scanner.mu.RUnlock()

	conn.mu.RLock()
	defer conn.mu.RUnlock()

	if conn.Variables["Temp"].Deadband == nil {
		t.Error("Temp should have deadband config")
	} else if conn.Variables["Temp"].Deadband.Value != 0.5 {
		t.Errorf("Temp deadband = %v, want 0.5", conn.Variables["Temp"].Deadband.Value)
	}
	if !conn.Variables["Status"].DisableRBE {
		t.Error("Status should have DisableRBE=true")
	}
}

func TestHandleSubscribeMissingFields(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.Start()
	defer scanner.Stop()

	tests := []struct {
		name string
		req  SubscribeRequest
	}{
		{"missing deviceId", SubscribeRequest{Host: "1.2.3.4", Tags: []string{"T"}, SubscriberID: "s"}},
		{"missing host", SubscribeRequest{DeviceID: "d", Tags: []string{"T"}, SubscriberID: "s"}},
		{"missing subscriberId", SubscribeRequest{DeviceID: "d", Host: "1.2.3.4", Tags: []string{"T"}}},
		{"missing tags", SubscribeRequest{DeviceID: "d", Host: "1.2.3.4", SubscriberID: "s"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, _ := json.Marshal(tt.req)
			resp, err := nc.Request("ethernetip.subscribe", data, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]interface{}
			json.Unmarshal(resp.Data, &result)
			if result["success"] != false {
				t.Errorf("expected success=false, got %v", result["success"])
			}
		})
	}
}

func TestHandleSubscribeInvalidJSON(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.Start()
	defer scanner.Stop()

	resp, err := nc.Request("ethernetip.subscribe", []byte("not json"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	json.Unmarshal(resp.Data, &result)
	if result["success"] != false {
		t.Errorf("expected success=false for invalid JSON, got %v", result["success"])
	}
}

func TestHandleUnsubscribe(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	// Subscribe first
	subReq := SubscribeRequest{
		DeviceID:     "plc1",
		Host:         "192.168.1.100",
		Tags:         []string{"Tag1", "Tag2", "Tag3"},
		SubscriberID: "sub1",
	}
	data, _ := json.Marshal(subReq)
	nc.Request("ethernetip.subscribe", data, 2*time.Second)

	// Unsubscribe one tag
	unsubReq := UnsubscribeRequest{
		DeviceID:     "plc1",
		Tags:         []string{"Tag1"},
		SubscriberID: "sub1",
	}
	data, _ = json.Marshal(unsubReq)
	resp, err := nc.Request("ethernetip.unsubscribe", data, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	json.Unmarshal(resp.Data, &result)
	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}

	// Connection should still exist
	scanner.mu.RLock()
	conn, exists := scanner.connections["plc1"]
	scanner.mu.RUnlock()
	if !exists {
		t.Fatal("connection should still exist")
	}

	conn.mu.RLock()
	if conn.Subscribers["sub1"]["Tag1"] {
		t.Error("Tag1 should be removed from subscriber")
	}
	if !conn.Subscribers["sub1"]["Tag2"] {
		t.Error("Tag2 should still exist")
	}
	conn.mu.RUnlock()

	// Unsubscribe remaining tags - connection should close
	unsubReq2 := UnsubscribeRequest{
		DeviceID:     "plc1",
		Tags:         []string{"Tag2", "Tag3"},
		SubscriberID: "sub1",
	}
	data, _ = json.Marshal(unsubReq2)
	nc.Request("ethernetip.unsubscribe", data, 2*time.Second)

	scanner.mu.RLock()
	_, exists = scanner.connections["plc1"]
	scanner.mu.RUnlock()
	if exists {
		t.Error("connection should be closed after all subscribers removed")
	}
}

func TestHandleUnsubscribeUnknownDevice(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.Start()
	defer scanner.Stop()

	req := UnsubscribeRequest{
		DeviceID:     "nonexistent",
		Tags:         []string{"Tag1"},
		SubscriberID: "sub1",
	}
	data, _ := json.Marshal(req)
	resp, err := nc.Request("ethernetip.unsubscribe", data, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	json.Unmarshal(resp.Data, &result)
	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}
	if result["count"].(float64) != 0 {
		t.Errorf("expected count=0, got %v", result["count"])
	}
}

func TestHandleVariables(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	// Subscribe to create variables
	subReq := SubscribeRequest{
		DeviceID:     "plc1",
		Host:         "192.168.1.100",
		Tags:         []string{"Tag1", "Tag2"},
		CipTypes:     map[string]string{"Tag1": "DINT", "Tag2": "REAL"},
		StructTypes:  map[string]string{"Tag1": "Timer"},
		SubscriberID: "sub1",
	}
	data, _ := json.Marshal(subReq)
	nc.Request("ethernetip.subscribe", data, 2*time.Second)

	// Set some values on the cached vars
	scanner.mu.RLock()
	conn := scanner.connections["plc1"]
	scanner.mu.RUnlock()

	conn.mu.Lock()
	conn.Variables["Tag1"].Value = int64(42)
	conn.Variables["Tag1"].Quality = "good"
	conn.Variables["Tag1"].Datatype = "number"
	conn.Variables["Tag2"].Value = float64(3.14)
	conn.Variables["Tag2"].Quality = "good"
	conn.Variables["Tag2"].Datatype = "number"
	conn.mu.Unlock()

	// Request variables
	resp, err := nc.Request("ethernetip.variables", nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var vars []VariableInfo
	if err := json.Unmarshal(resp.Data, &vars); err != nil {
		t.Fatal(err)
	}

	if len(vars) != 2 {
		t.Fatalf("expected 2 variables, got %d", len(vars))
	}

	// Find Tag1 and check struct type lookup
	found := false
	for _, v := range vars {
		if v.VariableID == "Tag1" {
			found = true
			if v.StructType != "Timer" {
				t.Errorf("Tag1 structType = %q, want Timer", v.StructType)
			}
			if v.Origin != "plc" {
				t.Errorf("Tag1 origin = %q, want plc", v.Origin)
			}
			if v.ModuleID != moduleID {
				t.Errorf("Tag1 moduleId = %q, want %s", v.ModuleID, moduleID)
			}
		}
	}
	if !found {
		t.Error("Tag1 not found in variables response")
	}
}

func TestHandleBrowseValidation(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.Start()
	defer scanner.Stop()

	t.Run("missing fields returns empty result", func(t *testing.T) {
		req := BrowseRequest{DeviceID: "plc1"} // missing Host
		data, _ := json.Marshal(req)
		resp, err := nc.Request("ethernetip.browse", data, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var result BrowseResult
		json.Unmarshal(resp.Data, &result)
		if len(result.Variables) != 0 {
			t.Error("expected empty variables")
		}
	})

	t.Run("invalid JSON returns empty result", func(t *testing.T) {
		resp, err := nc.Request("ethernetip.browse", []byte("bad"), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var result BrowseResult
		json.Unmarshal(resp.Data, &result)
		if len(result.Variables) != 0 {
			t.Error("expected empty variables")
		}
	})

	t.Run("valid request returns browseId", func(t *testing.T) {
		req := BrowseRequest{
			DeviceID: "plc1",
			Host:     "192.168.1.100",
			BrowseID: "test-browse-123",
		}
		data, _ := json.Marshal(req)
		resp, err := nc.Request("ethernetip.browse", data, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]string
		json.Unmarshal(resp.Data, &result)
		if result["browseId"] != "test-browse-123" {
			t.Errorf("browseId = %q, want test-browse-123", result["browseId"])
		}
	})
}

func TestHandleSubscribeDefaultScanRate(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	req := SubscribeRequest{
		DeviceID:     "plc-default",
		Host:         "192.168.1.100",
		Tags:         []string{"Tag1"},
		SubscriberID: "sub1",
		ScanRate:     0, // should default to 1000
	}
	data, _ := json.Marshal(req)
	nc.Request("ethernetip.subscribe", data, 2*time.Second)

	scanner.mu.RLock()
	conn := scanner.connections["plc-default"]
	scanner.mu.RUnlock()

	if conn.ScanRate != 1000*time.Millisecond {
		t.Errorf("ScanRate = %v, want 1s", conn.ScanRate)
	}
}

func TestHandleMultipleSubscribers(t *testing.T) {
	nc, cleanup := startTestNATS(t)
	defer cleanup()

	scanner := NewScanner(nc)
	scanner.SetEnabled(false)
	scanner.Start()
	defer scanner.Stop()

	// First subscriber
	req1 := SubscribeRequest{
		DeviceID:     "plc1",
		Host:         "192.168.1.100",
		Tags:         []string{"Tag1", "Tag2"},
		SubscriberID: "sub1",
	}
	data, _ := json.Marshal(req1)
	nc.Request("ethernetip.subscribe", data, 2*time.Second)

	// Second subscriber with overlapping tags
	req2 := SubscribeRequest{
		DeviceID:     "plc1",
		Host:         "192.168.1.100",
		Tags:         []string{"Tag2", "Tag3"},
		SubscriberID: "sub2",
	}
	data, _ = json.Marshal(req2)
	nc.Request("ethernetip.subscribe", data, 2*time.Second)

	scanner.mu.RLock()
	conn := scanner.connections["plc1"]
	scanner.mu.RUnlock()

	conn.mu.RLock()
	defer conn.mu.RUnlock()

	// Should have 3 unique variables
	if len(conn.Variables) != 3 {
		t.Errorf("expected 3 variables, got %d", len(conn.Variables))
	}
	// Both subscribers tracked
	if len(conn.Subscribers) != 2 {
		t.Errorf("expected 2 subscribers, got %d", len(conn.Subscribers))
	}
}
