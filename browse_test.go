package main

import (
	"encoding/binary"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════
// Test helpers for building binary buffers
// ═══════════════════════════════════════════════════════════════════════════

type tagListEntry struct {
	instanceID uint32
	symbolType uint16
	elemSize   uint16
	arrayDims  [3]uint32
	name       string
}

func buildTagListBuffer(entries []tagListEntry) []byte {
	var buf []byte
	for _, e := range entries {
		b := make([]byte, 4+2+2+12+2+len(e.name))
		binary.LittleEndian.PutUint32(b[0:], e.instanceID)
		binary.LittleEndian.PutUint16(b[4:], e.symbolType)
		binary.LittleEndian.PutUint16(b[6:], e.elemSize)
		binary.LittleEndian.PutUint32(b[8:], e.arrayDims[0])
		binary.LittleEndian.PutUint32(b[12:], e.arrayDims[1])
		binary.LittleEndian.PutUint32(b[16:], e.arrayDims[2])
		binary.LittleEndian.PutUint16(b[20:], uint16(len(e.name)))
		copy(b[22:], []byte(e.name))
		buf = append(buf, b...)
	}
	return buf
}

// ═══════════════════════════════════════════════════════════════════════════
// parseTagList
// ═══════════════════════════════════════════════════════════════════════════

func TestParseTagList(t *testing.T) {
	t.Run("parses atomic tags", func(t *testing.T) {
		buf := buildTagListBuffer([]tagListEntry{
			{instanceID: 1, symbolType: 0x00C4, elemSize: 4, name: "MyDINT"},
			{instanceID: 2, symbolType: 0x00CA, elemSize: 4, name: "MyREAL"},
		})
		tag := NewMockTagFromBytes(buf)

		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(tags) != 2 {
			t.Fatalf("expected 2 tags, got %d", len(tags))
		}
		if tags[0].Name != "MyDINT" {
			t.Errorf("tags[0].Name = %q, want MyDINT", tags[0].Name)
		}
		if tags[0].SymbolType != 0x00C4 {
			t.Errorf("tags[0].SymbolType = 0x%04X, want 0x00C4", tags[0].SymbolType)
		}
		if tags[1].Name != "MyREAL" {
			t.Errorf("tags[1].Name = %q, want MyREAL", tags[1].Name)
		}
	})

	t.Run("filters system tags", func(t *testing.T) {
		buf := buildTagListBuffer([]tagListEntry{
			{instanceID: 1, symbolType: 0x00C4, elemSize: 4, name: "UserTag"},
			{instanceID: 2, symbolType: 0x1000, elemSize: 4, name: "SystemTag"}, // system bit set
			{instanceID: 3, symbolType: 0x00CA, elemSize: 4, name: "AnotherTag"},
		})
		tag := NewMockTagFromBytes(buf)

		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(tags) != 2 {
			t.Fatalf("expected 2 non-system tags, got %d", len(tags))
		}
		if tags[0].Name != "UserTag" || tags[1].Name != "AnotherTag" {
			t.Errorf("unexpected tags: %v, %v", tags[0].Name, tags[1].Name)
		}
	})

	t.Run("parses struct tags", func(t *testing.T) {
		buf := buildTagListBuffer([]tagListEntry{
			{instanceID: 1, symbolType: 0x8123, elemSize: 40, name: "MyUDT"},
		})
		tag := NewMockTagFromBytes(buf)

		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(tags) != 1 {
			t.Fatalf("expected 1 tag, got %d", len(tags))
		}
		if !tags[0].IsStruct() {
			t.Error("expected IsStruct=true")
		}
		if tags[0].TemplateID() != 0x0123 {
			t.Errorf("templateID = 0x%04X, want 0x0123", tags[0].TemplateID())
		}
	})

	t.Run("handles array dims", func(t *testing.T) {
		buf := buildTagListBuffer([]tagListEntry{
			{instanceID: 1, symbolType: 0x00C4, elemSize: 4, arrayDims: [3]uint32{10, 0, 0}, name: "ArrayTag"},
		})
		tag := NewMockTagFromBytes(buf)

		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if tags[0].ArrayDims[0] != 10 {
			t.Errorf("arrayDims[0] = %d, want 10", tags[0].ArrayDims[0])
		}
	})

	t.Run("empty buffer", func(t *testing.T) {
		tag := NewMockTag(0)
		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(tags) != 0 {
			t.Errorf("expected 0 tags, got %d", len(tags))
		}
	})

	t.Run("truncated buffer stops gracefully", func(t *testing.T) {
		tag := NewMockTag(10) // too small for a full entry header (22 bytes)
		tags, err := parseTagList(tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(tags) != 0 {
			t.Errorf("expected 0 tags from truncated buffer, got %d", len(tags))
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// parseUdtTemplate
// ═══════════════════════════════════════════════════════════════════════════

// buildUdtBuffer constructs a binary @udt/<id> response for testing.
func buildUdtBuffer(udtID, memberCount uint16, instanceSize uint32, fields []UdtFieldDesc, names []string) []byte {
	// Header: 14 bytes
	// Field descriptors: memberCount * 8 bytes
	// Strings: null-terminated

	memberDescWords := uint32(memberCount) * 2 // each field is 8 bytes = 2 words
	descSize := int(memberCount) * 8
	stringOffset := 14 + descSize

	// Calculate string section size
	stringsSize := 0
	for _, n := range names {
		stringsSize += len(n) + 1 // null terminator
	}

	totalSize := stringOffset + stringsSize
	buf := make([]byte, totalSize)

	// Header
	binary.LittleEndian.PutUint16(buf[0:], udtID)
	binary.LittleEndian.PutUint32(buf[2:], memberDescWords)
	binary.LittleEndian.PutUint32(buf[6:], instanceSize)
	binary.LittleEndian.PutUint16(buf[10:], memberCount)
	binary.LittleEndian.PutUint16(buf[12:], 0) // struct handle

	// Field descriptors
	for i, f := range fields {
		off := 14 + i*8
		binary.LittleEndian.PutUint16(buf[off:], f.Metadata)
		binary.LittleEndian.PutUint16(buf[off+2:], f.TypeCode)
		binary.LittleEndian.PutUint32(buf[off+4:], f.Offset)
	}

	// Strings (null-terminated)
	off := stringOffset
	for _, n := range names {
		copy(buf[off:], []byte(n))
		off += len(n)
		buf[off] = 0
		off++
	}

	return buf
}

func TestParseUdtTemplate(t *testing.T) {
	t.Run("simple UDT with atomic fields", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x00C4, Offset: 0},  // DINT at offset 0
			{Metadata: 0, TypeCode: 0x00CA, Offset: 4},  // REAL at offset 4
			{Metadata: 0, TypeCode: 0x00C1, Offset: 8},  // BOOL at offset 8
		}
		names := []string{"MyTimer", "PRE", "ACC", "EN"} // UDT name + field names

		buf := buildUdtBuffer(100, 3, 12, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 100)
		if err != nil {
			t.Fatal(err)
		}

		if tmpl.Name != "MyTimer" {
			t.Errorf("name = %q, want MyTimer", tmpl.Name)
		}
		if tmpl.ID != 100 {
			t.Errorf("ID = %d, want 100", tmpl.ID)
		}
		if tmpl.MemberCount != 3 {
			t.Errorf("memberCount = %d, want 3", tmpl.MemberCount)
		}
		if len(tmpl.Fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(tmpl.Fields))
		}

		if tmpl.Fields[0].Name != "PRE" || tmpl.Fields[0].Datatype != "DINT" {
			t.Errorf("field[0] = %q/%q, want PRE/DINT", tmpl.Fields[0].Name, tmpl.Fields[0].Datatype)
		}
		if tmpl.Fields[1].Name != "ACC" || tmpl.Fields[1].Datatype != "REAL" {
			t.Errorf("field[1] = %q/%q, want ACC/REAL", tmpl.Fields[1].Name, tmpl.Fields[1].Datatype)
		}
		if tmpl.Fields[2].Name != "EN" || tmpl.Fields[2].Datatype != "BOOL" {
			t.Errorf("field[2] = %q/%q, want EN/BOOL", tmpl.Fields[2].Name, tmpl.Fields[2].Datatype)
		}
	})

	t.Run("UDT name with semicolon suffix", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x00C4, Offset: 0},
		}
		names := []string{"Timer;extra_info", "PRE"}

		buf := buildUdtBuffer(50, 1, 4, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 50)
		if err != nil {
			t.Fatal(err)
		}
		if tmpl.Name != "Timer" {
			t.Errorf("name = %q, want Timer (semicolon stripped)", tmpl.Name)
		}
	})

	t.Run("UDT name with colon suffix", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x00C4, Offset: 0},
		}
		names := []string{"Timer:1:0", "PRE"}

		buf := buildUdtBuffer(50, 1, 4, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 50)
		if err != nil {
			t.Fatal(err)
		}
		if tmpl.Name != "Timer" {
			t.Errorf("name = %q, want Timer (colon stripped)", tmpl.Name)
		}
	})

	t.Run("hidden fields marked", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x00C4, Offset: 0},
			{Metadata: 0, TypeCode: 0x00C4, Offset: 4},
			{Metadata: 0, TypeCode: 0x00C4, Offset: 8},
		}
		names := []string{"MyUDT", "Visible", "ZZZZZ_Hidden", "__internal"}

		buf := buildUdtBuffer(10, 3, 12, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 10)
		if err != nil {
			t.Fatal(err)
		}

		if tmpl.Fields[0].IsHidden {
			t.Error("Visible should not be hidden")
		}
		if !tmpl.Fields[1].IsHidden {
			t.Error("ZZZZZ_Hidden should be hidden")
		}
		if !tmpl.Fields[2].IsHidden {
			t.Error("__internal should be hidden")
		}
	})

	t.Run("nested struct field", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x8050, Offset: 0}, // struct with template 0x050
		}
		names := []string{"Outer", "Nested"}

		buf := buildUdtBuffer(10, 1, 40, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 10)
		if err != nil {
			t.Fatal(err)
		}

		if tmpl.Fields[0].Datatype != "STRUCT" {
			t.Errorf("datatype = %q, want STRUCT", tmpl.Fields[0].Datatype)
		}
		if !tmpl.Fields[0].Desc.IsStruct() {
			t.Error("expected IsStruct=true")
		}
		if tmpl.Fields[0].Desc.NestedTemplateID() != 0x0050 {
			t.Errorf("nestedTemplateID = 0x%04X, want 0x0050", tmpl.Fields[0].Desc.NestedTemplateID())
		}
	})

	t.Run("array field", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 10, TypeCode: 0x20C4, Offset: 0}, // array of DINT, 0x2000 = array bit
		}
		names := []string{"MyUDT", "ArrayField"}

		buf := buildUdtBuffer(10, 1, 40, fields, names)
		tag := NewMockTagFromBytes(buf)

		tmpl, err := parseUdtTemplate(tag, 10)
		if err != nil {
			t.Fatal(err)
		}

		if !tmpl.Fields[0].IsArray {
			t.Error("expected IsArray=true")
		}
	})

	t.Run("buffer too small", func(t *testing.T) {
		tag := NewMockTag(5) // less than 14 byte header
		_, err := parseUdtTemplate(tag, 10)
		if err == nil {
			t.Error("expected error for buffer too small")
		}
	})

	t.Run("fallback name when empty", func(t *testing.T) {
		fields := []UdtFieldDesc{
			{Metadata: 0, TypeCode: 0x00C4, Offset: 0},
		}
		// No strings in the buffer - use member desc words that point past end
		memberDescWords := uint32(1) * 2
		headerSize := 14
		descSize := 8

		buf := make([]byte, headerSize+descSize)
		binary.LittleEndian.PutUint16(buf[0:], 99)
		binary.LittleEndian.PutUint32(buf[2:], memberDescWords)
		binary.LittleEndian.PutUint32(buf[6:], 4)
		binary.LittleEndian.PutUint16(buf[10:], 1)
		binary.LittleEndian.PutUint16(buf[12:], 0)

		off := 14
		binary.LittleEndian.PutUint16(buf[off:], fields[0].Metadata)
		binary.LittleEndian.PutUint16(buf[off+2:], fields[0].TypeCode)
		binary.LittleEndian.PutUint32(buf[off+4:], fields[0].Offset)

		tag := NewMockTagFromBytes(buf)
		tmpl, err := parseUdtTemplate(tag, 99)
		if err != nil {
			t.Fatal(err)
		}
		if tmpl.Name != "Template_99" {
			t.Errorf("name = %q, want Template_99", tmpl.Name)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// readNullTerminatedStrings
// ═══════════════════════════════════════════════════════════════════════════

func TestReadNullTerminatedStrings(t *testing.T) {
	t.Run("multiple strings", func(t *testing.T) {
		data := []byte("Hello\x00World\x00Test\x00")
		tag := NewMockTagFromBytes(data)

		strings := readNullTerminatedStrings(tag, 0, len(data))
		if len(strings) != 3 {
			t.Fatalf("expected 3 strings, got %d: %v", len(strings), strings)
		}
		if strings[0] != "Hello" || strings[1] != "World" || strings[2] != "Test" {
			t.Errorf("got %v", strings)
		}
	})

	t.Run("string without trailing null", func(t *testing.T) {
		data := []byte("Hello\x00World")
		tag := NewMockTagFromBytes(data)

		strings := readNullTerminatedStrings(tag, 0, len(data))
		if len(strings) != 2 {
			t.Fatalf("expected 2 strings, got %d: %v", len(strings), strings)
		}
		if strings[0] != "Hello" || strings[1] != "World" {
			t.Errorf("got %v", strings)
		}
	})

	t.Run("with offset", func(t *testing.T) {
		data := []byte("SKIP\x00Hello\x00World\x00")
		tag := NewMockTagFromBytes(data)

		strings := readNullTerminatedStrings(tag, 5, len(data))
		if len(strings) != 2 {
			t.Fatalf("expected 2 strings, got %d: %v", len(strings), strings)
		}
		if strings[0] != "Hello" || strings[1] != "World" {
			t.Errorf("got %v", strings)
		}
	})

	t.Run("empty buffer", func(t *testing.T) {
		tag := NewMockTag(0)
		strings := readNullTerminatedStrings(tag, 0, 0)
		if len(strings) != 0 {
			t.Errorf("expected 0 strings, got %d", len(strings))
		}
	})

	t.Run("consecutive nulls skipped", func(t *testing.T) {
		data := []byte("A\x00\x00B\x00")
		tag := NewMockTagFromBytes(data)

		strings := readNullTerminatedStrings(tag, 0, len(data))
		if len(strings) != 2 {
			t.Fatalf("expected 2 strings (empty between nulls skipped), got %d: %v", len(strings), strings)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// expandMembers
// ═══════════════════════════════════════════════════════════════════════════

func TestExpandMembers(t *testing.T) {
	t.Run("expands atomic fields", func(t *testing.T) {
		tmpl := &UdtTemplate{
			Name: "Timer",
			Fields: []UdtField{
				{Name: "PRE", Datatype: "DINT", Desc: UdtFieldDesc{TypeCode: 0x00C4}},
				{Name: "ACC", Datatype: "DINT", Desc: UdtFieldDesc{TypeCode: 0x00C4}},
				{Name: "EN", Datatype: "BOOL", Desc: UdtFieldDesc{TypeCode: 0x00C1}},
			},
		}
		templates := map[uint16]*UdtTemplate{}

		var candidates []candidateVar
		expandMembers(&candidates, "T4", tmpl, templates, "dev1", "192.168.1.1", 44818, 0, 1)

		if len(candidates) != 3 {
			t.Fatalf("expected 3 candidates, got %d", len(candidates))
		}
		if candidates[0].path != "T4.PRE" {
			t.Errorf("candidates[0].path = %q, want T4.PRE", candidates[0].path)
		}
		if candidates[0].info.CipType != "DINT" {
			t.Errorf("candidates[0].cipType = %q, want DINT", candidates[0].info.CipType)
		}
		if candidates[2].path != "T4.EN" {
			t.Errorf("candidates[2].path = %q, want T4.EN", candidates[2].path)
		}
	})

	t.Run("skips hidden fields", func(t *testing.T) {
		tmpl := &UdtTemplate{
			Fields: []UdtField{
				{Name: "Visible", Datatype: "DINT", Desc: UdtFieldDesc{TypeCode: 0x00C4}},
				{Name: "ZZZZZ_Hidden", Datatype: "DINT", IsHidden: true, Desc: UdtFieldDesc{TypeCode: 0x00C4}},
				{Name: "__internal", Datatype: "DINT", IsHidden: true, Desc: UdtFieldDesc{TypeCode: 0x00C4}},
			},
		}

		var candidates []candidateVar
		expandMembers(&candidates, "T", tmpl, map[uint16]*UdtTemplate{}, "d", "h", 44818, 0, 1)

		if len(candidates) != 1 {
			t.Errorf("expected 1 candidate (hidden skipped), got %d", len(candidates))
		}
	})

	t.Run("respects max depth", func(t *testing.T) {
		inner := &UdtTemplate{
			Fields: []UdtField{
				{Name: "Value", Datatype: "DINT", Desc: UdtFieldDesc{TypeCode: 0x00C4}},
			},
		}
		outer := &UdtTemplate{
			Fields: []UdtField{
				{Name: "Inner", Datatype: "STRUCT", Desc: UdtFieldDesc{TypeCode: 0x8050}},
			},
		}
		templates := map[uint16]*UdtTemplate{0x050: inner}

		// maxDepth=1, depth=0 → will expand Inner, but Inner.Value is at depth=1 which equals maxDepth, so stop
		var candidates []candidateVar
		expandMembers(&candidates, "T", outer, templates, "d", "h", 44818, 0, 1)

		// At depth 0, it finds Inner (STRUCT), recurses with depth=1.
		// At depth 1, depth >= maxDepth → returns immediately.
		if len(candidates) != 0 {
			t.Errorf("expected 0 candidates at max depth, got %d", len(candidates))
		}

		// maxDepth=2 → should expand one level
		candidates = nil
		expandMembers(&candidates, "T", outer, templates, "d", "h", 44818, 0, 2)
		if len(candidates) != 1 {
			t.Fatalf("expected 1 candidate, got %d", len(candidates))
		}
		if candidates[0].path != "T.Inner.Value" {
			t.Errorf("path = %q, want T.Inner.Value", candidates[0].path)
		}
	})

	t.Run("sets correct variable info", func(t *testing.T) {
		tmpl := &UdtTemplate{
			Fields: []UdtField{
				{Name: "Temp", Datatype: "REAL", Desc: UdtFieldDesc{TypeCode: 0x00CA}},
			},
		}

		var candidates []candidateVar
		expandMembers(&candidates, "Sensor", tmpl, map[uint16]*UdtTemplate{}, "plc1", "10.0.0.1", 44818, 0, 1)

		if len(candidates) != 1 {
			t.Fatal("expected 1 candidate")
		}
		c := candidates[0]
		if c.info.DeviceID != "plc1" {
			t.Errorf("deviceId = %q, want plc1", c.info.DeviceID)
		}
		if c.info.ModuleID != moduleID {
			t.Errorf("moduleId = %q, want %s", c.info.ModuleID, moduleID)
		}
		if c.info.Datatype != "number" {
			t.Errorf("datatype = %q, want number", c.info.Datatype)
		}
		if c.info.Quality != "unknown" {
			t.Errorf("quality = %q, want unknown", c.info.Quality)
		}
	})
}
