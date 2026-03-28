package main

import "strings"

// ═══════════════════════════════════════════════════════════════════════════
// NATS Request/Response payloads
// ═══════════════════════════════════════════════════════════════════════════

// BrowseRequest is the JSON payload for ethernetip.browse requests.
type BrowseRequest struct {
	DeviceID string `json:"deviceId"`
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"`
	BrowseID string `json:"browseId,omitempty"`
	Async    bool   `json:"async,omitempty"`
}

// DeadBandConfig defines RBE (Report By Exception) thresholds for a tag.
type DeadBandConfig struct {
	Value   float64 `json:"value"`            // only publish if change exceeds this amount
	MinTime int64   `json:"minTime,omitempty"` // ms — suppress publishes more frequent than this
	MaxTime int64   `json:"maxTime,omitempty"` // ms — force publish if exceeded, 0 = disabled
}

// SubscribeRequest is the JSON payload for ethernetip.subscribe requests.
type SubscribeRequest struct {
	DeviceID     string                     `json:"deviceId"`
	Host         string                     `json:"host"`
	Port         int                        `json:"port,omitempty"`
	Tags         []string                   `json:"tags"`
	CipTypes     map[string]string          `json:"cipTypes,omitempty"`     // tag name → CIP type (e.g. "REAL", "DINT")
	StructTypes  map[string]string          `json:"structTypes,omitempty"`  // base tag name → UDT template name (e.g. "Analog_Input")
	Deadbands    map[string]DeadBandConfig  `json:"deadbands,omitempty"`    // tag name → deadband config
	DisableRBE   map[string]bool            `json:"disableRBE,omitempty"`   // tag name → true to force publish all
	ScanRate     int                        `json:"scanRate,omitempty"`
	SubscriberID string                     `json:"subscriberId"`
}

// UnsubscribeRequest is the JSON payload for ethernetip.unsubscribe requests.
type UnsubscribeRequest struct {
	DeviceID     string   `json:"deviceId"`
	Tags         []string `json:"tags"`
	SubscriberID string   `json:"subscriberId"`
}

// ═══════════════════════════════════════════════════════════════════════════
// Published message types
// ═══════════════════════════════════════════════════════════════════════════

// PlcDataMessage is published on ethernetip.data.{deviceId}.{sanitizedTag}
// when a monitored tag changes value (or RBE forces publish).
type PlcDataMessage struct {
	ModuleID    string          `json:"moduleId"`
	DeviceID    string          `json:"deviceId"`
	VariableID  string          `json:"variableId"`
	Value       interface{}     `json:"value"`
	Timestamp   int64           `json:"timestamp"`
	Datatype    string          `json:"datatype"`
	Description string          `json:"description,omitempty"`
	Deadband    *DeadBandConfig `json:"deadband,omitempty"`
	DisableRBE  bool            `json:"disableRBE,omitempty"`
}

// ServiceHeartbeat is published every 10s to the service_heartbeats KV bucket.
type ServiceHeartbeat struct {
	ServiceType string                 `json:"serviceType"`
	ModuleID    string                 `json:"moduleId"`
	LastSeen    int64                  `json:"lastSeen"`
	StartedAt   int64                  `json:"startedAt"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ServiceEnabledKV is the value stored in the service_enabled KV bucket.
type ServiceEnabledKV struct {
	ModuleID  string `json:"moduleId"`
	Enabled   bool   `json:"enabled"`
	UpdatedAt int64  `json:"updatedAt"`
}

// BrowseProgressMessage is published during async browse operations
// to ethernetip.browse.progress.{browseId}.
type BrowseProgressMessage struct {
	BrowseID      string `json:"browseId"`
	ModuleID      string `json:"moduleId"`
	DeviceID      string `json:"deviceId"`
	Phase         string `json:"phase"` // "discovering", "expanding", "reading", "caching", "completed", "failed"
	TotalTags     int    `json:"totalTags"`
	CompletedTags int    `json:"completedTags"`
	ErrorCount    int    `json:"errorCount"`
	Message       string `json:"message,omitempty"`
	Timestamp     int64  `json:"timestamp"`
}

// ServiceLogEntry is published to service.logs.ethernetip.ethernetip for log streaming.
type ServiceLogEntry struct {
	Timestamp   int64  `json:"timestamp"`
	Level       string `json:"level"`
	Message     string `json:"message"`
	ServiceType string `json:"serviceType"`
	ModuleID    string `json:"moduleId"`
	Logger      string `json:"logger,omitempty"`
}

// ═══════════════════════════════════════════════════════════════════════════
// Variable info (returned by browse and variables requests)
// ═══════════════════════════════════════════════════════════════════════════

// VariableInfo is the JSON structure returned for individual tags.
type VariableInfo struct {
	ModuleID    string      `json:"moduleId"`
	DeviceID    string      `json:"deviceId"`
	VariableID  string      `json:"variableId"`
	Value       interface{} `json:"value"`
	Datatype    string      `json:"datatype"`
	CipType     string      `json:"cipType,omitempty"`
	StructType  string      `json:"structType,omitempty"` // UDT template name (e.g. "Analog_Input")
	Quality     string      `json:"quality"`
	Origin      string      `json:"origin"`
	LastUpdated int64       `json:"lastUpdated"`
}

// ═══════════════════════════════════════════════════════════════════════════
// UDT types (returned by browse for codegen)
// ═══════════════════════════════════════════════════════════════════════════

// UdtMemberExport describes a single member of a UDT template.
type UdtMemberExport struct {
	Name    string `json:"name"`
	Datatype string `json:"datatype"`
	UdtType string `json:"udtType,omitempty"`
	IsArray bool   `json:"isArray"`
}

// UdtExport describes a UDT template definition.
type UdtExport struct {
	Name    string            `json:"name"`
	Members []UdtMemberExport `json:"members"`
}

// BrowseResult is the full browse response with UDT info.
type BrowseResult struct {
	Variables  []VariableInfo       `json:"variables"`
	Udts       map[string]UdtExport `json:"udts"`
	StructTags map[string]string    `json:"structTags"`
}

// ═══════════════════════════════════════════════════════════════════════════
// Tag listing types (parsed from @tags response)
// ═══════════════════════════════════════════════════════════════════════════

// TagEntry represents a single tag from the @tags listing.
type TagEntry struct {
	Name       string
	SymbolType uint16
	ElemSize   uint16
	ArrayDims  [3]uint32
}

// IsStruct returns true if the tag is a structure/UDT type.
func (t TagEntry) IsStruct() bool {
	return t.SymbolType&0x8000 != 0
}

// IsSystem returns true if the tag is a system-internal type.
func (t TagEntry) IsSystem() bool {
	return t.SymbolType&0x1000 != 0
}

// TemplateID returns the UDT template ID (lower 12 bits) if this is a struct.
func (t TagEntry) TemplateID() uint16 {
	return t.SymbolType & 0x0FFF
}

// UdtFieldDesc is a raw field descriptor from the @udt response.
type UdtFieldDesc struct {
	Metadata uint16 // array count or bit number
	TypeCode uint16 // same bitmask as SymbolType
	Offset   uint32 // byte offset in structure
}

// IsStruct returns true if this field is a nested structure.
func (f UdtFieldDesc) IsStruct() bool {
	return f.TypeCode&0x8000 != 0
}

// IsArray returns true if this field is an array.
func (f UdtFieldDesc) IsArray() bool {
	return f.TypeCode&0x2000 != 0
}

// NestedTemplateID returns the template ID if this is a struct field.
func (f UdtFieldDesc) NestedTemplateID() uint16 {
	return f.TypeCode & 0x0FFF
}

// UdtTemplate is a parsed UDT template from @udt/<id>.
type UdtTemplate struct {
	ID           uint16
	Name         string
	InstanceSize uint32
	MemberCount  uint16
	Fields       []UdtField
}

// UdtField is a fully resolved field in a UDT template.
type UdtField struct {
	Name     string
	Desc     UdtFieldDesc
	Datatype string // "BOOL", "DINT", "REAL", "STRUCT", etc.
	UdtName  string // If STRUCT, the resolved template name
	IsArray  bool
	IsHidden bool // ZZZZZ* or __* fields
}

// ═══════════════════════════════════════════════════════════════════════════
// CIP type mapping
// ═══════════════════════════════════════════════════════════════════════════

// CipTypeInfo maps a CIP type code to its name and size.
type CipTypeInfo struct {
	Name string
	Size int
}

// cipTypes maps CIP type codes to their names and sizes.
var cipTypes = map[uint16]CipTypeInfo{
	0xC1: {"BOOL", 1},
	0xC2: {"SINT", 1},
	0xC3: {"INT", 2},
	0xC4: {"DINT", 4},
	0xC5: {"LINT", 8},
	0xC6: {"USINT", 1},
	0xC7: {"UINT", 2},
	0xC8: {"UDINT", 4},
	0xC9: {"ULINT", 8},
	0xCA: {"REAL", 4},
	0xCB: {"LREAL", 8},
	0xCC: {"STIME", 4},
	0xCD: {"DATE", 2},
	0xCE: {"TIME_OF_DAY", 4},
	0xCF: {"DATE_AND_TIME", 8},
	0xD0: {"STRING", 88},
	0xD1: {"BYTE", 1},
	0xD2: {"WORD", 2},
	0xD3: {"DWORD", 4},
	0xD4: {"LWORD", 8},
	0xA0: {"BIT_STRING", 4},
}

// cipToNatsDatatype normalizes CIP type names to "number", "boolean", or "string".
func cipToNatsDatatype(cipType string) string {
	switch cipType {
	case "BOOL":
		return "boolean"
	case "SINT", "INT", "DINT", "LINT",
		"USINT", "UINT", "UDINT", "ULINT",
		"REAL", "LREAL",
		"STIME", "DATE", "TIME_OF_DAY", "DATE_AND_TIME",
		"BYTE", "WORD", "DWORD", "LWORD", "BIT_STRING":
		return "number"
	case "STRING":
		return "string"
	default:
		return "string"
	}
}

// resolveCipType returns the CIP type name for a given type code.
func resolveCipType(typeCode uint16) string {
	if info, ok := cipTypes[typeCode]; ok {
		return info.Name
	}
	return "UNKNOWN"
}

// sanitizeDeviceIdForSubject replaces spaces and NATS-invalid characters in a device ID.
func sanitizeDeviceIdForSubject(id string) string {
	r := strings.NewReplacer(" ", "_", ".", "_", "*", "_", ">", "_")
	return r.Replace(id)
}

// sanitizeTagForSubject converts a tag name to a valid NATS subject segment.
// "MyTag.Member" → "MyTag_Member"
func sanitizeTagForSubject(tag string) string {
	r := strings.NewReplacer(".", "_", ":", "_", "[", "_", "]", "_")
	return r.Replace(tag)
}
