package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	common "github.com/joyautomation/tentacle-go-common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const moduleID = "ethernetip"
const serviceType = "ethernetip"

// natsLogger publishes log entries to NATS in addition to stdout.
type natsLogger struct {
	nc          *nats.Conn
	subject     string
	serviceType string
	moduleID    string
	mu          sync.Mutex
}

func (l *natsLogger) publish(level, logger, msg string) {
	if l.nc == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := common.ServiceLogEntry{
		Timestamp:   time.Now().UnixMilli(),
		Level:       level,
		Message:     msg,
		ServiceType: l.serviceType,
		ModuleID:    l.moduleID,
		Logger:      logger,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	_ = l.nc.Publish(l.subject, data)
}

var natsLog *natsLogger

func logInfo(logger, msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	slog.Info(formatted, "logger", logger)
	if natsLog != nil {
		natsLog.publish("info", logger, formatted)
	}
}

func logWarn(logger, msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	slog.Warn(formatted, "logger", logger)
	if natsLog != nil {
		natsLog.publish("warn", logger, formatted)
	}
}

func logError(logger, msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	slog.Error(formatted, "logger", logger)
	if natsLog != nil {
		natsLog.publish("error", logger, formatted)
	}
}

func logDebug(logger, msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	slog.Debug(formatted, "logger", logger)
	if natsLog != nil {
		natsLog.publish("debug", logger, formatted)
	}
}

// connectToNats connects to NATS with infinite retry.
func connectToNats(servers string) (*nats.Conn, error) {
	for {
		slog.Info(fmt.Sprintf("Connecting to NATS at %s...", servers))
		nc, err := nats.Connect(servers,
			nats.MaxReconnects(-1),
			nats.ReconnectWait(5*time.Second),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				if err != nil {
					slog.Warn(fmt.Sprintf("NATS disconnected: %v", err))
				}
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				slog.Info("NATS reconnected")
			}),
		)
		if err != nil {
			slog.Warn(fmt.Sprintf("Failed to connect to NATS: %v. Retrying in 5 seconds...", err))
			time.Sleep(5 * time.Second)
			continue
		}
		slog.Info("Connected to NATS")
		return nc, nil
	}
}

func main() {
	slog.Info("═══════════════════════════════════════════════════════════════")
	slog.Info("            tentacle-ethernetip Service (Go + libplctag)")
	slog.Info("═══════════════════════════════════════════════════════════════")

	natsServers := os.Getenv("NATS_SERVERS")
	if natsServers == "" {
		natsServers = "nats://localhost:4222"
	}

	slog.Info(fmt.Sprintf("Module ID: %s", moduleID))
	slog.Info(fmt.Sprintf("NATS Servers: %s", natsServers))

	// Connect to NATS (retries forever)
	nc, err := connectToNats(natsServers)
	if err != nil {
		slog.Error(fmt.Sprintf("NATS connection failed: %v", err))
		os.Exit(1)
	}

	// Enable NATS log streaming
	natsLog = &natsLogger{
		nc:          nc,
		subject:     fmt.Sprintf("service.logs.%s.%s", serviceType, moduleID),
		serviceType: serviceType,
		moduleID:    moduleID,
	}

	// Create and start scanner
	logInfo("eip", "Initializing EtherNet/IP scanner (libplctag)...")
	scanner := NewScanner(nc)
	scanner.Start()

	// ═══════════════════════════════════════════════════════════════════════════
	// Heartbeat publishing
	// ═══════════════════════════════════════════════════════════════════════════

	js, err := jetstream.New(nc)
	if err != nil {
		logError("eip", "Failed to create JetStream context: %v", err)
		os.Exit(1)
	}

	ctx := context.Background()
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "service_heartbeats",
		History: 1,
		TTL:     60 * time.Second,
	})
	if err != nil {
		logError("eip", "Failed to create/open heartbeat KV: %v", err)
		os.Exit(1)
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// Service enabled/disabled state
	// ═══════════════════════════════════════════════════════════════════════════

	enabledKv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "service_enabled",
		History: 1,
		TTL:     0, // No expiration
	})
	if err != nil {
		logError("eip", "Failed to create/open service_enabled KV: %v", err)
		os.Exit(1)
	}

	// Check initial enabled state
	if entry, err := enabledKv.Get(ctx, moduleID); err == nil {
		var state common.ServiceEnabledKV
		if json.Unmarshal(entry.Value(), &state) == nil {
			scanner.SetEnabled(state.Enabled)
			logInfo("eip", "Initial enabled state: %v", state.Enabled)
		}
	}

	// Watch for enabled state changes
	enabledWatcher, err := enabledKv.Watch(ctx, moduleID)
	if err != nil {
		logWarn("eip", "Failed to watch service_enabled KV: %v", err)
	} else {
		go func() {
			for entry := range enabledWatcher.Updates() {
				if entry == nil {
					continue
				}
				if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
					scanner.SetEnabled(true) // deleted = default to enabled
					continue
				}
				var state common.ServiceEnabledKV
				if json.Unmarshal(entry.Value(), &state) == nil {
					scanner.SetEnabled(state.Enabled)
				}
			}
		}()
	}

	startedAt := time.Now().UnixMilli()

	publishHeartbeat := func() {
		devices := scanner.ActiveDevices()
		devicesJSON, _ := json.Marshal(devices)
		hb := common.ServiceHeartbeat{
			ServiceType: serviceType,
			ModuleID:    moduleID,
			LastSeen:    time.Now().UnixMilli(),
			StartedAt:   startedAt,
			Metadata: map[string]interface{}{
				"devices":     string(devicesJSON),
				"enabled":     scanner.IsEnabled(),
				"publishRate": fmt.Sprintf("%.1f", scanner.PollRate()),
			},
		}
		data, err := json.Marshal(hb)
		if err != nil {
			logWarn("eip", "Failed to marshal heartbeat: %v", err)
			return
		}
		if _, err := kv.Put(ctx, moduleID, data); err != nil {
			logWarn("eip", "Failed to publish heartbeat: %v", err)
		}
	}

	// Publish initial heartbeat
	publishHeartbeat()
	logInfo("eip", "Service heartbeat started (moduleId: %s)", moduleID)

	// Publish heartbeat every 10 seconds
	heartbeatTicker := time.NewTicker(10 * time.Second)
	go func() {
		for range heartbeatTicker.C {
			publishHeartbeat()
		}
	}()

	logInfo("eip", "")
	logInfo("eip", "Service running. Press Ctrl+C to stop.")
	logInfo("eip", "Waiting for subscribe/browse requests...")
	logInfo("eip", "")

	// ═══════════════════════════════════════════════════════════════════════════
	// Shutdown handling
	// ═══════════════════════════════════════════════════════════════════════════

	shutdown := func(reason string) {
		logInfo("eip", "Received %s, shutting down...", reason)

		heartbeatTicker.Stop()
		if enabledWatcher != nil {
			_ = enabledWatcher.Stop()
		}
		if err := kv.Delete(ctx, moduleID); err != nil {
			// Ignore — may already be expired
		}
		logInfo("eip", "Removed service heartbeat")

		scanner.Stop()

		if err := nc.Drain(); err != nil {
			logWarn("eip", "NATS drain error: %v", err)
		}

		logInfo("eip", "Shutdown complete")
	}

	// Listen for NATS shutdown command
	shutdownSub, err := nc.Subscribe(moduleID+".shutdown", func(msg *nats.Msg) {
		logInfo("eip", "Received shutdown command via NATS")
		shutdown("NATS shutdown")
		os.Exit(0)
	})
	if err != nil {
		logWarn("eip", "Failed to subscribe to shutdown: %v", err)
	}

	// Listen for OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	_ = shutdownSub

	shutdown(sig.String())
}
