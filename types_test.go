package main

import "testing"

func TestCipToNatsDatatype(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"BOOL", "boolean"},
		{"SINT", "number"},
		{"INT", "number"},
		{"DINT", "number"},
		{"LINT", "number"},
		{"USINT", "number"},
		{"UINT", "number"},
		{"UDINT", "number"},
		{"ULINT", "number"},
		{"REAL", "number"},
		{"LREAL", "number"},
		{"STIME", "number"},
		{"DATE", "number"},
		{"TIME_OF_DAY", "number"},
		{"DATE_AND_TIME", "number"},
		{"BYTE", "number"},
		{"WORD", "number"},
		{"DWORD", "number"},
		{"LWORD", "number"},
		{"BIT_STRING", "number"},
		{"STRING", "string"},
		{"UNKNOWN", "string"},
		{"", "string"},
		{"CUSTOM_TYPE", "string"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := cipToNatsDatatype(tt.input)
			if got != tt.want {
				t.Errorf("cipToNatsDatatype(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveCipType(t *testing.T) {
	tests := []struct {
		code uint16
		want string
	}{
		{0xC1, "BOOL"},
		{0xC2, "SINT"},
		{0xC3, "INT"},
		{0xC4, "DINT"},
		{0xC5, "LINT"},
		{0xC6, "USINT"},
		{0xC7, "UINT"},
		{0xC8, "UDINT"},
		{0xC9, "ULINT"},
		{0xCA, "REAL"},
		{0xCB, "LREAL"},
		{0xD0, "STRING"},
		{0xA0, "BIT_STRING"},
		{0x00, "UNKNOWN"},
		{0xFF, "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := resolveCipType(tt.code)
			if got != tt.want {
				t.Errorf("resolveCipType(0x%X) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}

func TestSanitizeTagForSubject(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"MyTag", "MyTag"},
		{"MyTag.Member", "MyTag_Member"},
		{"Array[0]", "Array_0_"},
		{"Tag:Port", "Tag_Port"},
		{"A.B:C[1]", "A_B_C_1_"},
		{"Simple", "Simple"},
		{"a.b.c.d", "a_b_c_d"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeTagForSubject(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeTagForSubject(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestTagEntryIsStruct(t *testing.T) {
	tests := []struct {
		symbolType uint16
		want       bool
	}{
		{0x8000, true},
		{0x8123, true},
		{0x00C4, false},
		{0x0000, false},
		{0x7FFF, false},
	}
	for _, tt := range tests {
		tag := TagEntry{SymbolType: tt.symbolType}
		if got := tag.IsStruct(); got != tt.want {
			t.Errorf("TagEntry{SymbolType: 0x%04X}.IsStruct() = %v, want %v", tt.symbolType, got, tt.want)
		}
	}
}

func TestTagEntryIsSystem(t *testing.T) {
	tests := []struct {
		symbolType uint16
		want       bool
	}{
		{0x1000, true},
		{0x1234, true},
		{0x9000, true}, // both struct and system
		{0x00C4, false},
		{0x0000, false},
	}
	for _, tt := range tests {
		tag := TagEntry{SymbolType: tt.symbolType}
		if got := tag.IsSystem(); got != tt.want {
			t.Errorf("TagEntry{SymbolType: 0x%04X}.IsSystem() = %v, want %v", tt.symbolType, got, tt.want)
		}
	}
}

func TestTagEntryTemplateID(t *testing.T) {
	tests := []struct {
		symbolType uint16
		want       uint16
	}{
		{0x8123, 0x0123},
		{0x80FF, 0x00FF},
		{0x0FFF, 0x0FFF},
		{0xFFFF, 0x0FFF},
		{0x0000, 0x0000},
	}
	for _, tt := range tests {
		tag := TagEntry{SymbolType: tt.symbolType}
		if got := tag.TemplateID(); got != tt.want {
			t.Errorf("TagEntry{SymbolType: 0x%04X}.TemplateID() = 0x%04X, want 0x%04X", tt.symbolType, got, tt.want)
		}
	}
}

func TestUdtFieldDescIsStruct(t *testing.T) {
	if !(UdtFieldDesc{TypeCode: 0x8000}).IsStruct() {
		t.Error("expected IsStruct for TypeCode 0x8000")
	}
	if (UdtFieldDesc{TypeCode: 0x00C4}).IsStruct() {
		t.Error("expected not IsStruct for TypeCode 0x00C4")
	}
}

func TestUdtFieldDescIsArray(t *testing.T) {
	if !(UdtFieldDesc{TypeCode: 0x2000}).IsArray() {
		t.Error("expected IsArray for TypeCode 0x2000")
	}
	if (UdtFieldDesc{TypeCode: 0x00C4}).IsArray() {
		t.Error("expected not IsArray for TypeCode 0x00C4")
	}
}

func TestUdtFieldDescNestedTemplateID(t *testing.T) {
	f := UdtFieldDesc{TypeCode: 0x8123}
	if got := f.NestedTemplateID(); got != 0x0123 {
		t.Errorf("NestedTemplateID() = 0x%04X, want 0x0123", got)
	}
}

func TestIsPrintable(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"Hello World!", true},
		{"ABCabc123", true},
		{"~!@#$%", true},
		{" ", true},
		{"", true}, // vacuously true
		{"Hello\x00World", false},
		{"Tab\t", false},
		{"\n", false},
		{"\x7f", false},
		{"\x19", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isPrintable(tt.input); got != tt.want {
				t.Errorf("isPrintable(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
