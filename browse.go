package main

import (
	"fmt"
	"strings"
	"time"
)

const browseTimeout = 30 * time.Second

// listTags reads all controller-scoped tags from the PLC using @tags.
func listTags(gateway string, port int) ([]TagEntry, error) {
	attrs := buildListTagAttrs(gateway, port)
	tag, err := createTag(attrs, browseTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create @tags tag: %w", err)
	}
	defer tag.Close()

	if err := tag.Read(browseTimeout); err != nil {
		return nil, fmt.Errorf("failed to read @tags: %w", err)
	}

	return parseTagList(tag)
}

// parseTagList parses the raw @tags response buffer into TagEntry structs.
func parseTagList(tag *PlcTag) ([]TagEntry, error) {
	size := tag.Size()
	offset := 0
	var tags []TagEntry

	for offset < size {
		if offset+22 > size {
			break // Not enough data for another entry header
		}

		// Skip instance_id (4 bytes)
		offset += 4

		symbolType := tag.GetUint16(offset)
		offset += 2

		elemSize := tag.GetUint16(offset)
		offset += 2

		var arrayDims [3]uint32
		arrayDims[0] = tag.GetUint32(offset)
		offset += 4
		arrayDims[1] = tag.GetUint32(offset)
		offset += 4
		arrayDims[2] = tag.GetUint32(offset)
		offset += 4

		strLen := int(tag.GetUint16(offset))
		offset += 2

		if offset+strLen > size {
			break
		}

		nameBytes := tag.GetRawBytes(offset, strLen)
		name := string(nameBytes)
		offset += strLen

		// Skip system tags and program-scoped containers
		if symbolType&0x1000 != 0 {
			continue
		}

		tags = append(tags, TagEntry{
			Name:       name,
			SymbolType: symbolType,
			ElemSize:   elemSize,
			ArrayDims:  arrayDims,
		})
	}

	return tags, nil
}

// readUdtTemplate reads a UDT template definition using @udt/<id>.
func readUdtTemplate(gateway string, port int, templateID uint16) (*UdtTemplate, error) {
	attrs := buildUdtAttrs(gateway, port, templateID)
	tag, err := createTag(attrs, browseTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create @udt/%d tag: %w", templateID, err)
	}
	defer tag.Close()

	if err := tag.Read(browseTimeout); err != nil {
		return nil, fmt.Errorf("failed to read @udt/%d: %w", templateID, err)
	}

	return parseUdtTemplate(tag, templateID)
}

// parseUdtTemplate parses the raw @udt/<id> response buffer.
//
// Binary format:
//   Header (14 bytes):
//     [0:2]   UDT ID (uint16)
//     [2:6]   Member desc size in 32-bit words (uint32)
//     [6:10]  Instance size in bytes (uint32)
//     [10:12] Number of members (uint16)
//     [12:14] Struct handle (uint16)
//   Field descriptors (N * 8 bytes each):
//     [0:2]   Metadata (uint16) - array count or bit number
//     [2:4]   Type code (uint16)
//     [4:8]   Byte offset (uint32)
//   UDT name (null-terminated string, may have ";extra" suffix)
//   Field names (N null-terminated strings)
func parseUdtTemplate(tag *PlcTag, templateID uint16) (*UdtTemplate, error) {
	size := tag.Size()
	if size < 14 {
		return nil, fmt.Errorf("@udt/%d response too small: %d bytes", templateID, size)
	}

	// Parse header
	// udtID := tag.GetUint16(0) // Should match templateID
	memberDescWords := tag.GetUint32(2)
	instanceSize := tag.GetUint32(6)
	memberCount := tag.GetUint16(10)
	// structHandle := tag.GetUint16(12)

	// Parse field descriptors
	descOffset := 14
	descs := make([]UdtFieldDesc, 0, memberCount)
	for i := 0; i < int(memberCount); i++ {
		off := descOffset + i*8
		if off+8 > size {
			break
		}
		descs = append(descs, UdtFieldDesc{
			Metadata: tag.GetUint16(off),
			TypeCode: tag.GetUint16(off + 2),
			Offset:   tag.GetUint32(off + 4),
		})
	}

	// String section starts after field descriptors
	// memberDescWords is in 32-bit words and includes field descriptors
	stringOffset := 14 + int(memberDescWords)*4
	if stringOffset >= size {
		// Fallback: calculate from member count
		stringOffset = descOffset + int(memberCount)*8
	}

	// Read all null-terminated strings from string section
	allStrings := readNullTerminatedStrings(tag, stringOffset, size)

	// First string is the UDT name (may contain ";extra" suffix)
	udtName := ""
	var fieldNames []string
	if len(allStrings) > 0 {
		udtName = allStrings[0]
		// Truncate at semicolon
		if idx := strings.IndexByte(udtName, ';'); idx >= 0 {
			udtName = udtName[:idx]
		}
		// Also truncate at first colon (version info like "Name:1:0")
		if idx := strings.IndexByte(udtName, ':'); idx >= 0 {
			udtName = udtName[:idx]
		}
		fieldNames = allStrings[1:]
	}

	if udtName == "" {
		udtName = fmt.Sprintf("Template_%d", templateID)
	}

	// Build fields by pairing descriptors with names
	fields := make([]UdtField, 0, len(descs))
	for i, desc := range descs {
		name := ""
		if i < len(fieldNames) {
			name = fieldNames[i]
		} else {
			name = fmt.Sprintf("_member%d", i)
		}

		isHidden := strings.HasPrefix(name, "ZZZZZ") || strings.HasPrefix(name, "__")
		isArray := desc.IsArray()

		var datatype, udtType string
		if desc.IsStruct() {
			datatype = "STRUCT"
		} else {
			rawType := desc.TypeCode & 0x00FF
			if info, ok := cipTypes[rawType]; ok {
				datatype = info.Name
			} else {
				datatype = "UNKNOWN"
			}
		}

		fields = append(fields, UdtField{
			Name:     name,
			Desc:     desc,
			Datatype: datatype,
			UdtName:  udtType,
			IsArray:  isArray,
			IsHidden: isHidden,
		})
	}

	return &UdtTemplate{
		ID:           templateID,
		Name:         udtName,
		InstanceSize: instanceSize,
		MemberCount:  memberCount,
		Fields:       fields,
	}, nil
}

// readNullTerminatedStrings reads all null-terminated strings from offset to end.
func readNullTerminatedStrings(tag *PlcTag, offset, size int) []string {
	var strings []string
	start := offset

	for i := offset; i < size; i++ {
		b := tag.GetUint8(i)
		if b == 0 {
			if i > start {
				buf := tag.GetRawBytes(start, i-start)
				strings = append(strings, string(buf))
			}
			start = i + 1
		}
	}

	// Handle last string if not null-terminated
	if start < size {
		buf := tag.GetRawBytes(start, size-start)
		s := string(buf)
		if len(s) > 0 && isPrintable(s) {
			strings = append(strings, s)
		}
	}

	return strings
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 0x20 || r > 0x7e {
			return false
		}
	}
	return true
}

// browseDevice lists all tags and reads all UDT templates from a device.
// Returns the full BrowseResult ready for JSON serialization.
func browseDevice(gateway string, port int, deviceID string, browseID string, publishProgress func(BrowseProgressMessage)) (*BrowseResult, error) {
	if port == 0 {
		port = 44818
	}

	// Phase 1: List all tags
	publishProgress(BrowseProgressMessage{
		BrowseID:  browseID,
		ModuleID:  moduleID,
		DeviceID:  deviceID,
		Phase:     "discovering",
		Message:   "Listing all tags...",
		Timestamp: time.Now().UnixMilli(),
	})

	tags, err := listTags(gateway, port)
	if err != nil {
		return nil, fmt.Errorf("tag listing failed: %w", err)
	}

	logInfo("eip", "Listed %d tags from %s:%d", len(tags), gateway, port)

	publishProgress(BrowseProgressMessage{
		BrowseID:  browseID,
		ModuleID:  moduleID,
		DeviceID:  deviceID,
		Phase:     "discovering",
		TotalTags: len(tags),
		Message:   fmt.Sprintf("Found %d tags", len(tags)),
		Timestamp: time.Now().UnixMilli(),
	})

	// Phase 2: Collect all unique UDT template IDs
	templateIDs := make(map[uint16]bool)
	for _, t := range tags {
		if t.IsStruct() && !t.IsSystem() {
			templateIDs[t.TemplateID()] = true
		}
	}

	logInfo("eip", "Found %d unique UDT template IDs", len(templateIDs))

	// Phase 3: Read all UDT templates (recursively discovers nested UDTs)
	publishProgress(BrowseProgressMessage{
		BrowseID:  browseID,
		ModuleID:  moduleID,
		DeviceID:  deviceID,
		Phase:     "expanding",
		TotalTags: len(tags),
		Message:   fmt.Sprintf("Reading %d UDT templates...", len(templateIDs)),
		Timestamp: time.Now().UnixMilli(),
	})

	templates := make(map[uint16]*UdtTemplate)
	toProcess := make([]uint16, 0, len(templateIDs))
	for id := range templateIDs {
		toProcess = append(toProcess, id)
	}

	for len(toProcess) > 0 {
		id := toProcess[0]
		toProcess = toProcess[1:]

		if _, done := templates[id]; done {
			continue
		}

		tmpl, err := readUdtTemplate(gateway, port, id)
		if err != nil {
			logWarn("eip", "Failed to read UDT template %d: %v", id, err)
			continue
		}

		templates[id] = tmpl
		logInfo("eip", "Read UDT template %d: %s (%d fields)", id, tmpl.Name, len(tmpl.Fields))

		// Queue nested struct types for reading
		for _, field := range tmpl.Fields {
			if field.Desc.IsStruct() {
				nestedID := field.Desc.NestedTemplateID()
				if _, done := templates[nestedID]; !done {
					toProcess = append(toProcess, nestedID)
				}
			}
		}
	}

	// Build templateID → name lookup
	templateIDToName := make(map[uint16]string)
	for id, tmpl := range templates {
		templateIDToName[id] = tmpl.Name
	}

	// Resolve nested UDT names in fields
	for _, tmpl := range templates {
		for i, field := range tmpl.Fields {
			if field.Desc.IsStruct() {
				nestedID := field.Desc.NestedTemplateID()
				if name, ok := templateIDToName[nestedID]; ok {
					tmpl.Fields[i].UdtName = name
				}
			}
		}
	}

	// Phase 4: Build browse result
	publishProgress(BrowseProgressMessage{
		BrowseID:      browseID,
		ModuleID:      moduleID,
		DeviceID:      deviceID,
		Phase:         "reading",
		TotalTags:     len(tags),
		CompletedTags: len(tags),
		Message:       "Building browse result...",
		Timestamp:     time.Now().UnixMilli(),
	})

	// Build variables list (expand struct tags into member paths)
	variables := make([]VariableInfo, 0, len(tags)*2)
	structTags := make(map[string]string)

	for _, t := range tags {
		// Skip Rockwell internal UDT default-value holders
		if strings.HasPrefix(t.Name, "__DEFVAL_") {
			continue
		}

		if t.IsStruct() {
			tid := t.TemplateID()
			tmpl, ok := templates[tid]
			if !ok {
				continue
			}

			structTags[t.Name] = tmpl.Name

			// Expand only first-level struct members (deeper levels can't be read individually)
			expandMembers(&variables, t.Name, tmpl, templates, deviceID, gateway, port, 0, 1)
		} else {
			// Atomic tag
			rawType := t.SymbolType & 0x00FF
			cipType := resolveCipType(rawType)
			natsType := cipToNatsDatatype(cipType)

			variables = append(variables, VariableInfo{
				ModuleID:    moduleID,
				DeviceID:    deviceID,
				VariableID:  t.Name,
				Value:       nil,
				Datatype:    natsType,
				CipType:     cipType,
				Quality:     "unknown",
				Origin:      "plc",
				LastUpdated: 0,
			})
		}
	}

	// Build UDT exports
	udts := make(map[string]UdtExport)
	for _, tmpl := range templates {
		members := make([]UdtMemberExport, 0)
		for _, field := range tmpl.Fields {
			if field.IsHidden {
				continue
			}
			natsType := cipToNatsDatatype(field.Datatype)
			if field.Datatype == "STRUCT" {
				natsType = "STRUCT"
			}
			members = append(members, UdtMemberExport{
				Name:     field.Name,
				Datatype: natsType,
				UdtType:  field.UdtName,
				IsArray:  field.IsArray,
			})
		}
		udts[tmpl.Name] = UdtExport{
			Name:    tmpl.Name,
			Members: members,
		}
	}

	result := &BrowseResult{
		Variables:  variables,
		Udts:       udts,
		StructTags: structTags,
	}

	publishProgress(BrowseProgressMessage{
		BrowseID:      browseID,
		ModuleID:      moduleID,
		DeviceID:      deviceID,
		Phase:         "completed",
		TotalTags:     len(variables),
		CompletedTags: len(variables),
		Message:       fmt.Sprintf("Browse complete: %d variables, %d UDTs", len(variables), len(udts)),
		Timestamp:     time.Now().UnixMilli(),
	})

	return result, nil
}

// expandMembers recursively expands a struct tag's members into flat variable entries.
func expandMembers(variables *[]VariableInfo, basePath string, tmpl *UdtTemplate, templates map[uint16]*UdtTemplate, deviceID string, gateway string, port int, depth, maxDepth int) {
	if depth >= maxDepth {
		return
	}

	for _, field := range tmpl.Fields {
		if field.IsHidden {
			continue
		}

		memberPath := basePath + "." + field.Name

		if field.Datatype == "STRUCT" && field.Desc.IsStruct() {
			nestedID := field.Desc.NestedTemplateID()
			if nestedTmpl, ok := templates[nestedID]; ok {
				expandMembers(variables, memberPath, nestedTmpl, templates, deviceID, gateway, port, depth+1, maxDepth)
				continue
			}
		}

		natsType := cipToNatsDatatype(field.Datatype)

		// Test if this member tag is readable by the PLC — skip unreadable internal AOI members
		if !testTagReadable(gateway, port, memberPath) {
			logDebug("eip", "Skipping unreadable member %s during browse", memberPath)
			continue
		}

		*variables = append(*variables, VariableInfo{
			ModuleID:    moduleID,
			DeviceID:    deviceID,
			VariableID:  memberPath,
			Value:       nil,
			Datatype:    natsType,
			CipType:     field.Datatype,
			Quality:     "unknown",
			Origin:      "plc",
			LastUpdated: 0,
		})
	}
}

// testTagReadable does a quick create+read to check if a tag path is accessible on the PLC.
// Returns true if readable, false if the PLC rejects it (e.g. internal AOI members).
func testTagReadable(gateway string, port int, tagPath string) bool {
	attrs := buildTagAttrs(gateway, port, tagPath, 0)
	tag, err := createTag(attrs, 5*time.Second)
	if err != nil {
		return false
	}
	defer tag.Close()
	if err := tag.Read(5 * time.Second); err != nil {
		return false
	}
	return true
}
