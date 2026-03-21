package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// DeviceConnection holds the state for a connected EtherNet/IP device.
type DeviceConnection struct {
	DeviceID    string
	Gateway     string
	Port        int
	Variables   map[string]*CachedVar
	StructTypes map[string]string              // base tag name → UDT template name (from browse)
	Subscribers map[string]map[string]bool // subscriberID → set of tag names
	ScanRate    time.Duration
	stopChan    chan struct{}
	mu          sync.RWMutex
}

// CachedVar holds the cached state of a single tag.
type CachedVar struct {
	TagName            string
	Datatype           string // "number", "boolean", "string"
	CipType            string // "DINT", "REAL", etc.
	Value              interface{}
	Quality            string
	LastRead           int64
	TagHandle          *PlcTag          // persistent libplctag handle — created once, reused on every poll
	CreateFails        int              // consecutive handle creation failures — stop retrying after maxCreateRetries
	Deadband           *DeadBandConfig  // RBE deadband config (nil = publish on any change)
	DisableRBE         bool             // force publish all values
	LastPublishedValue interface{}      // last value that was actually published
	LastPublishedTime  int64            // unix ms when last published
}

const maxCreateRetries = 3 // stop trying to create handle after this many consecutive failures

// Scanner manages EtherNet/IP device connections and NATS request handling.
type Scanner struct {
	nc          *nats.Conn
	connections map[string]*DeviceConnection
	mu          sync.RWMutex

	browseSub      *nats.Subscription
	subscribeSub   *nats.Subscription
	unsubscribeSub *nats.Subscription
	variablesSub   *nats.Subscription
	commandSub     *nats.Subscription
}

// NewScanner creates a new EtherNet/IP scanner.
func NewScanner(nc *nats.Conn) *Scanner {
	return &Scanner{
		nc:          nc,
		connections: make(map[string]*DeviceConnection),
	}
}

// Start subscribes to all NATS request subjects.
func (s *Scanner) Start() {
	var err error

	s.browseSub, err = s.nc.Subscribe("ethernetip.browse", s.handleBrowse)
	if err != nil {
		logError("eip", "Failed to subscribe to ethernetip.browse: %v", err)
	}
	logInfo("eip", "Listening for browse requests on ethernetip.browse")

	s.subscribeSub, err = s.nc.Subscribe("ethernetip.subscribe", s.handleSubscribe)
	if err != nil {
		logError("eip", "Failed to subscribe to ethernetip.subscribe: %v", err)
	}
	logInfo("eip", "Listening for subscribe requests on ethernetip.subscribe")

	s.unsubscribeSub, err = s.nc.Subscribe("ethernetip.unsubscribe", s.handleUnsubscribe)
	if err != nil {
		logError("eip", "Failed to subscribe to ethernetip.unsubscribe: %v", err)
	}

	s.variablesSub, err = s.nc.Subscribe("ethernetip.variables", s.handleVariables)
	if err != nil {
		logError("eip", "Failed to subscribe to ethernetip.variables: %v", err)
	}

	s.commandSub, err = s.nc.Subscribe("ethernetip.command.>", s.handleCommand)
	if err != nil {
		logError("eip", "Failed to subscribe to ethernetip.command.>: %v", err)
	}
}

// ActiveDevice describes a currently connected device for heartbeat metadata.
type ActiveDevice struct {
	DeviceID string `json:"deviceId"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	TagCount int    `json:"tagCount"`
}

// ActiveDevices returns info about all currently connected devices.
func (s *Scanner) ActiveDevices() []ActiveDevice {
	s.mu.RLock()
	defer s.mu.RUnlock()

	devices := make([]ActiveDevice, 0, len(s.connections))
	for _, conn := range s.connections {
		conn.mu.RLock()
		devices = append(devices, ActiveDevice{
			DeviceID: conn.DeviceID,
			Host:     conn.Gateway,
			Port:     conn.Port,
			TagCount: len(conn.Variables),
		})
		conn.mu.RUnlock()
	}
	return devices
}

// Stop unsubscribes and closes all connections.
func (s *Scanner) Stop() {
	if s.browseSub != nil {
		_ = s.browseSub.Unsubscribe()
	}
	if s.subscribeSub != nil {
		_ = s.subscribeSub.Unsubscribe()
	}
	if s.unsubscribeSub != nil {
		_ = s.unsubscribeSub.Unsubscribe()
	}
	if s.variablesSub != nil {
		_ = s.variablesSub.Unsubscribe()
	}
	if s.commandSub != nil {
		_ = s.commandSub.Unsubscribe()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.connections {
		close(conn.stopChan)
		// Destroy all persistent tag handles
		conn.mu.Lock()
		for _, v := range conn.Variables {
			if v.TagHandle != nil {
				v.TagHandle.Close()
				v.TagHandle = nil
			}
		}
		conn.mu.Unlock()
	}
	s.connections = make(map[string]*DeviceConnection)
	logInfo("eip", "All connections closed")
}

// ═══════════════════════════════════════════════════════════════════════════
// Browse handler
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) handleBrowse(msg *nats.Msg) {
	var req BrowseRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respondJSON(msg, BrowseResult{Variables: []VariableInfo{}, Udts: map[string]UdtExport{}, StructTags: map[string]string{}})
		return
	}

	if req.DeviceID == "" || req.Host == "" {
		s.respondJSON(msg, BrowseResult{Variables: []VariableInfo{}, Udts: map[string]UdtExport{}, StructTags: map[string]string{}})
		return
	}

	browseID := req.BrowseID
	if browseID == "" {
		browseID = uuid.New().String()
	}

	publishProgress := func(progress BrowseProgressMessage) {
		subject := fmt.Sprintf("ethernetip.browse.progress.%s", browseID)
		data, _ := json.Marshal(progress)
		_ = s.nc.Publish(subject, data)
	}

	// Always async: respond immediately with browseID, run browse in background,
	// and publish result on the progress topic when complete.
	s.respondJSON(msg, map[string]string{"browseId": browseID})
	go func() {
		result, err := browseDevice(req.Host, req.Port, req.DeviceID, browseID, publishProgress)
		if err != nil {
			logError("eip", "Browse failed for %s: %v", req.DeviceID, err)
			publishProgress(BrowseProgressMessage{
				BrowseID:  browseID,
				ModuleID:  moduleID,
				DeviceID:  req.DeviceID,
				Phase:     "failed",
				Message:   fmt.Sprintf("Browse failed: %v", err),
				Timestamp: time.Now().UnixMilli(),
			})
			return
		}
		logInfo("eip", "Browse complete for %s: %d vars, %d UDTs", req.DeviceID, len(result.Variables), len(result.Udts))
		// Publish the result on the browse result topic so callers can retrieve it
		resultSubject := fmt.Sprintf("ethernetip.browse.result.%s", browseID)
		resultData, _ := json.Marshal(result)
		_ = s.nc.Publish(resultSubject, resultData)
	}()
}

// ═══════════════════════════════════════════════════════════════════════════
// Subscribe handler
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) handleSubscribe(msg *nats.Msg) {
	var req SubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respondJSON(msg, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}

	if req.DeviceID == "" || req.Host == "" || req.SubscriberID == "" || len(req.Tags) == 0 {
		s.respondJSON(msg, map[string]interface{}{"success": false, "error": "missing required fields"})
		return
	}

	scanRate := req.ScanRate
	if scanRate <= 0 {
		scanRate = 1000
	}

	s.mu.Lock()
	conn, exists := s.connections[req.DeviceID]
	if !exists {
		conn = &DeviceConnection{
			DeviceID:    req.DeviceID,
			Gateway:     req.Host,
			Port:        req.Port,
			Variables:   make(map[string]*CachedVar),
			StructTypes: make(map[string]string),
			Subscribers: make(map[string]map[string]bool),
			ScanRate:    time.Duration(scanRate) * time.Millisecond,
			stopChan:    make(chan struct{}),
		}
		s.connections[req.DeviceID] = conn
		logInfo("eip", "Created connection for device %s (%s:%d)", req.DeviceID, req.Host, req.Port)
	}
	s.mu.Unlock()

	conn.mu.Lock()
	// Merge structTypes from subscribe request
	if req.StructTypes != nil {
		for baseName, udtName := range req.StructTypes {
			conn.StructTypes[baseName] = udtName
		}
	}
	// Add subscriber
	if conn.Subscribers[req.SubscriberID] == nil {
		conn.Subscribers[req.SubscriberID] = make(map[string]bool)
	}

	addedCount := 0
	for _, tagName := range req.Tags {
		conn.Subscribers[req.SubscriberID][tagName] = true

		if _, exists := conn.Variables[tagName]; !exists {
			cipType := ""
			if req.CipTypes != nil {
				cipType = req.CipTypes[tagName]
			}
			cv := &CachedVar{
				TagName: tagName,
				CipType: cipType,
				Quality: "unknown",
			}
			// Apply RBE config from subscribe request
			if req.Deadbands != nil {
				if db, ok := req.Deadbands[tagName]; ok {
					cv.Deadband = &db
				}
			}
			if req.DisableRBE != nil {
				if disable, ok := req.DisableRBE[tagName]; ok {
					cv.DisableRBE = disable
				}
			}
			// Don't create tag handles here — pollOnce creates them lazily.
			// This keeps subscribe fast (no blocking on network I/O).
			conn.Variables[tagName] = cv
			addedCount++
		} else {
			// Update RBE config for existing variables (subscriber may have changed config)
			existing := conn.Variables[tagName]
			if req.Deadbands != nil {
				if db, ok := req.Deadbands[tagName]; ok {
					existing.Deadband = &db
				}
			}
			if req.DisableRBE != nil {
				if disable, ok := req.DisableRBE[tagName]; ok {
					existing.DisableRBE = disable
				}
			}
		}
	}
	conn.mu.Unlock()

	// Start polling if this is a new connection
	if !exists {
		go s.pollDevice(conn)
	}

	logInfo("eip", "Subscriber %s added %d tags to device %s (total: %d)", req.SubscriberID, len(req.Tags), req.DeviceID, len(conn.Variables))

	s.respondJSON(msg, map[string]interface{}{
		"success": true,
		"count":   len(req.Tags),
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Unsubscribe handler
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) handleUnsubscribe(msg *nats.Msg) {
	var req UnsubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respondJSON(msg, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}

	s.mu.RLock()
	conn, exists := s.connections[req.DeviceID]
	s.mu.RUnlock()

	if !exists {
		s.respondJSON(msg, map[string]interface{}{"success": true, "count": 0})
		return
	}

	conn.mu.Lock()
	if subs, ok := conn.Subscribers[req.SubscriberID]; ok {
		for _, tag := range req.Tags {
			delete(subs, tag)
		}
		if len(subs) == 0 {
			delete(conn.Subscribers, req.SubscriberID)
		}
	}

	// If no subscribers remain, shut down the connection
	shouldClose := len(conn.Subscribers) == 0
	conn.mu.Unlock()

	if shouldClose {
		s.mu.Lock()
		close(conn.stopChan)
		// Destroy all persistent tag handles for this connection
		conn.mu.Lock()
		for _, v := range conn.Variables {
			if v.TagHandle != nil {
				v.TagHandle.Close()
				v.TagHandle = nil
			}
		}
		conn.mu.Unlock()
		delete(s.connections, req.DeviceID)
		s.mu.Unlock()
		logInfo("eip", "Closed connection for device %s (no subscribers)", req.DeviceID)
	}

	s.respondJSON(msg, map[string]interface{}{
		"success": true,
		"count":   len(req.Tags),
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Variables handler
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) handleVariables(msg *nats.Msg) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var allVars []VariableInfo
	for _, conn := range s.connections {
		conn.mu.RLock()
		for _, v := range conn.Variables {
			vi := VariableInfo{
				ModuleID:    moduleID,
				DeviceID:    conn.DeviceID,
				VariableID:  v.TagName,
				Value:       v.Value,
				Datatype:    v.Datatype,
				CipType:     v.CipType,
				Quality:     v.Quality,
				Origin:      "plc",
				LastUpdated: v.LastRead,
			}
			// Look up UDT type name from structTypes — check base name (before first dot)
			baseName := v.TagName
			if dotIdx := strings.IndexByte(v.TagName, '.'); dotIdx != -1 {
				baseName = v.TagName[:dotIdx]
			}
			if udtName, ok := conn.StructTypes[baseName]; ok {
				vi.StructType = udtName
			}
			allVars = append(allVars, vi)
		}
		conn.mu.RUnlock()
	}

	s.respondJSON(msg, allVars)
}

// ═══════════════════════════════════════════════════════════════════════════
// Command (write) handler
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) handleCommand(msg *nats.Msg) {
	// Subject: ethernetip.command.{tagName}
	// Extract tag name from subject
	subject := msg.Subject
	prefix := "ethernetip.command."
	if len(subject) <= len(prefix) {
		return
	}
	tagName := subject[len(prefix):]

	// Find which connection has this tag
	s.mu.RLock()
	var targetConn *DeviceConnection
	for _, conn := range s.connections {
		conn.mu.RLock()
		if _, exists := conn.Variables[tagName]; exists {
			targetConn = conn
			conn.mu.RUnlock()
			break
		}
		conn.mu.RUnlock()
	}
	s.mu.RUnlock()

	if targetConn == nil {
		logWarn("eip", "Command for unknown tag: %s", tagName)
		return
	}

	// Write value using libplctag
	attrs := buildTagAttrs(targetConn.Gateway, targetConn.Port, tagName, 0)
	tag, err := createTag(attrs, 10*time.Second)
	if err != nil {
		logError("eip", "Failed to create tag for write %s: %v", tagName, err)
		return
	}
	defer tag.Close()

	// Parse command value and write
	valueStr := string(msg.Data)
	targetConn.mu.RLock()
	cachedVar := targetConn.Variables[tagName]
	cipType := ""
	if cachedVar != nil {
		cipType = cachedVar.CipType
	}
	targetConn.mu.RUnlock()

	if err := writeTagValue(tag, cipType, valueStr); err != nil {
		logError("eip", "Failed to write %s: %v", tagName, err)
		return
	}

	logInfo("eip", "Wrote %s = %s", tagName, valueStr)
}

// ═══════════════════════════════════════════════════════════════════════════
// RBE (Report By Exception) filtering
// ═══════════════════════════════════════════════════════════════════════════

// shouldPublish determines whether a new value should be published based on
// the tag's RBE configuration. Returns true if the value should be published.
func shouldPublish(v *CachedVar, newValue interface{}, nowMs int64) bool {
	// DisableRBE = force publish everything
	if v.DisableRBE {
		return true
	}

	// First read — always publish
	if v.LastPublishedTime == 0 {
		return true
	}

	// No deadband configured — publish on any change
	if v.Deadband == nil {
		return !valuesEqual(v.LastPublishedValue, newValue)
	}

	elapsed := nowMs - v.LastPublishedTime

	// MaxTime exceeded — force publish regardless of change or minTime
	if v.Deadband.MaxTime > 0 && elapsed >= v.Deadband.MaxTime {
		return true
	}

	// MinTime not yet elapsed — suppress even if value changed enough
	if v.Deadband.MinTime > 0 && elapsed < v.Deadband.MinTime {
		return false
	}

	// Numeric deadband check
	oldFloat, oldOk := toFloat64(v.LastPublishedValue)
	newFloat, newOk := toFloat64(newValue)
	if oldOk && newOk {
		return math.Abs(newFloat-oldFloat) > v.Deadband.Value
	}

	// Non-numeric (bool/string) — publish on any change
	return !valuesEqual(v.LastPublishedValue, newValue)
}

// valuesEqual compares two tag values for equality.
func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Normalize numeric types for comparison
	af, aOk := toFloat64(a)
	bf, bOk := toFloat64(b)
	if aOk && bOk {
		return af == bf
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// toFloat64 converts a numeric interface{} to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Polling loop
// ═══════════════════════════════════════════════════════════════════════════

func (s *Scanner) pollDevice(conn *DeviceConnection) {
	logInfo("eip", "Starting poll loop for device %s at %v", conn.DeviceID, conn.ScanRate)

	ticker := time.NewTicker(conn.ScanRate)
	defer ticker.Stop()

	for {
		select {
		case <-conn.stopChan:
			logInfo("eip", "Poll loop stopped for device %s", conn.DeviceID)
			return
		case <-ticker.C:
			s.pollOnce(conn)
		}
	}
}

func (s *Scanner) pollOnce(conn *DeviceConnection) {
	pollStart := time.Now()

	conn.mu.RLock()
	type tagWork struct {
		name    string
		cipType string
		handle  *PlcTag
	}
	var work []tagWork
	var needsHandle []string
	for name, v := range conn.Variables {
		if v.TagHandle == nil {
			if v.CreateFails < maxCreateRetries {
				needsHandle = append(needsHandle, name)
			}
		} else {
			work = append(work, tagWork{name: name, cipType: v.CipType, handle: v.TagHandle})
		}
	}
	conn.mu.RUnlock()

	// Create missing handles (sequential — only needed for new/failed tags)
	for _, tagName := range needsHandle {
		attrs := buildTagAttrs(conn.Gateway, conn.Port, tagName, 0)
		handle, err := createTag(attrs, 10*time.Second)
		conn.mu.Lock()
		v := conn.Variables[tagName]
		if err != nil {
			if v != nil {
				v.CreateFails++
				v.Quality = "bad"
				if v.CreateFails >= maxCreateRetries {
					logDebug("eip", "Skipping unreadable tag %s.%s after %d failures: %v", conn.DeviceID, tagName, v.CreateFails, err)
				}
			}
		} else if v != nil {
			v.TagHandle = handle
			v.CreateFails = 0
			work = append(work, tagWork{name: tagName, cipType: v.CipType, handle: handle})
		}
		conn.mu.Unlock()
	}

	if len(work) == 0 {
		return
	}

	// Parallel reads — fan out to goroutines, limited concurrency
	const maxConcurrent = 64
	sem := make(chan struct{}, maxConcurrent)
	type tagResult struct {
		name    string
		value   interface{}
		cipType string
		natsType string
	}
	results := make(chan tagResult, len(work))

	var wg sync.WaitGroup
	for _, tw := range work {
		wg.Add(1)
		go func(tw tagWork) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			if err := tw.handle.Read(10 * time.Second); err != nil {
				conn.mu.Lock()
				if v, ok := conn.Variables[tw.name]; ok {
					v.Quality = "bad"
					v.TagHandle.Close()
					v.TagHandle = nil
					v.CreateFails = 0
				}
				conn.mu.Unlock()
				return
			}

			var value interface{}
			var cipType string
			var readErr error
			if tw.cipType != "" {
				value, cipType, readErr = readByKnownType(tw.handle, tw.cipType)
			} else {
				value, cipType, readErr = readBySize(tw.handle)
			}
			if readErr != nil {
				conn.mu.Lock()
				if v, ok := conn.Variables[tw.name]; ok {
					v.Quality = "bad"
				}
				conn.mu.Unlock()
				return
			}

			results <- tagResult{name: tw.name, value: value, cipType: cipType, natsType: cipToNatsDatatype(cipType)}
		}(tw)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results and publish
	now := time.Now().UnixMilli()
	published := 0
	suppressed := 0
	for r := range results {
		conn.mu.Lock()
		v, ok := conn.Variables[r.name]
		if ok {
			v.Value = r.value
			v.Datatype = r.natsType
			if v.CipType == "" {
				v.CipType = r.cipType
			}
			v.Quality = "good"
			v.LastRead = now
		}

		// Apply RBE filtering
		if ok && !shouldPublish(v, r.value, now) {
			suppressed++
			conn.mu.Unlock()
			continue
		}

		// Update last-published state
		if ok {
			v.LastPublishedValue = r.value
			v.LastPublishedTime = now
		}
		conn.mu.Unlock()

		dataMsg := PlcDataMessage{
			ModuleID:   moduleID,
			DeviceID:   conn.DeviceID,
			VariableID: r.name,
			Value:      r.value,
			Timestamp:  now,
			Datatype:   r.natsType,
		}
		if ok && v.Deadband != nil {
			dataMsg.Deadband = v.Deadband
		}
		if ok && v.DisableRBE {
			dataMsg.DisableRBE = true
		}
		data, _ := json.Marshal(dataMsg)
		subject := fmt.Sprintf("ethernetip.data.%s.%s", conn.DeviceID, sanitizeTagForSubject(r.name))
		_ = s.nc.Publish(subject, data)
		published++
	}

	logInfo("eip", "Poll cycle for %s: %d published, %d suppressed (RBE), %d total in %v", conn.DeviceID, published, suppressed, len(work), time.Since(pollStart))
}

// ═══════════════════════════════════════════════════════════════════════════
// Tag read/write helpers
// ═══════════════════════════════════════════════════════════════════════════

// readBySize extracts a value from an already-read tag handle using size heuristics.
// Used as fallback when CIP type is not known from browse.
func readBySize(tag *PlcTag) (interface{}, string, error) {
	size := tag.Size()
	switch {
	case size == 1:
		val := tag.GetInt8(0)
		if val == 0 || val == 1 {
			return val != 0, "BOOL", nil
		}
		return int64(val), "SINT", nil
	case size == 2:
		return int64(tag.GetInt16(0)), "INT", nil
	case size == 4:
		// Ambiguous: could be DINT or REAL. Default to DINT.
		return int64(tag.GetInt32(0)), "DINT", nil
	case size == 8:
		return tag.GetFloat64(0), "LREAL", nil
	default:
		if size >= 4 {
			strLen := int(tag.GetInt32(0))
			if strLen > 0 && strLen <= size-4 {
				return tag.GetString(0), "STRING", nil
			}
		}
		return nil, "UNKNOWN", fmt.Errorf("unknown tag size: %d", size)
	}
}

// readByKnownType reads a tag value using the exact CIP type from browse.
func readByKnownType(tag *PlcTag, cipType string) (interface{}, string, error) {
	switch cipType {
	case "BOOL":
		return tag.GetBit(0), "BOOL", nil
	case "SINT":
		return int64(tag.GetInt8(0)), "SINT", nil
	case "INT":
		return int64(tag.GetInt16(0)), "INT", nil
	case "DINT":
		return int64(tag.GetInt32(0)), "DINT", nil
	case "LINT":
		return tag.GetInt64(0), "LINT", nil
	case "USINT":
		return int64(tag.GetUint8(0)), "USINT", nil
	case "UINT":
		return int64(tag.GetUint16(0)), "UINT", nil
	case "UDINT":
		return int64(tag.GetUint32(0)), "UDINT", nil
	case "REAL":
		return float64(tag.GetFloat32(0)), "REAL", nil
	case "LREAL":
		return tag.GetFloat64(0), "LREAL", nil
	case "STRING":
		return tag.GetString(0), "STRING", nil
	default:
		// Fall back to size heuristic for unknown types
		size := tag.Size()
		if size == 4 {
			return int64(tag.GetInt32(0)), cipType, nil
		}
		return nil, cipType, fmt.Errorf("unsupported CIP type: %s", cipType)
	}
}

// writeTagValue writes a value to a tag based on its CIP type.
func writeTagValue(tag *PlcTag, cipType string, valueStr string) error {
	// Read first to size the buffer
	if err := tag.Read(10 * time.Second); err != nil {
		return fmt.Errorf("pre-read failed: %w", err)
	}

	switch cipType {
	case "BOOL":
		val := valueStr == "true" || valueStr == "1"
		tag.SetBit(0, val)
	case "SINT":
		var n int64
		fmt.Sscanf(valueStr, "%d", &n)
		tag.SetInt32(0, int32(int8(n)))
	case "INT":
		var n int64
		fmt.Sscanf(valueStr, "%d", &n)
		tag.SetInt32(0, int32(int16(n)))
	case "DINT", "LINT":
		var n int64
		fmt.Sscanf(valueStr, "%d", &n)
		tag.SetInt32(0, int32(n))
	case "REAL":
		var f float64
		fmt.Sscanf(valueStr, "%f", &f)
		tag.SetFloat32(0, float32(f))
	case "LREAL":
		var f float64
		fmt.Sscanf(valueStr, "%f", &f)
		tag.SetFloat64(0, f)
	default:
		return fmt.Errorf("unsupported write type: %s", cipType)
	}

	return tag.Write(10 * time.Second)
}

// respondJSON marshals v to JSON and responds on the NATS message.
func (s *Scanner) respondJSON(msg *nats.Msg, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		logError("eip", "Failed to marshal response: %v", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		logError("eip", "Failed to respond: %v", err)
	}
}
