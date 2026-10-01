// sensor-archiver subscribes to MQTT topics and archives every received message to daily
// compressed JSON Lines files. It uses a persistent MQTT session and acknowledges
// messages only after they have been fsynced, so restarts and crashes don't lose
// messages (but may duplicate a few).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	maxBatch      = 500
	statsInterval = time.Hour
)

type config struct {
	broker, username, password, clientID, topic, dir string
}

type received struct {
	msg mqtt.Message
	at  time.Time
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	archive, err := OpenArchive(cfg.dir, time.Now(), log)
	if err != nil {
		return err
	}

	msgs := make(chan received, 10000)
	fatal := make(chan error, 1)
	done := make(chan struct{}) // Closed when writeLoop stops consuming msgs
	client := mqtt.NewClient(mqttOptions(cfg, log, msgs, done, fatal))
	log.Info("connecting", "broker", cfg.broker, "client_id", cfg.clientID, "topic", cfg.topic, "dir", cfg.dir)
	client.Connect() // Retries in the background until connected

	writeErr := writeLoop(ctx, archive, msgs, fatal, log)
	close(done)

	// Unwritten messages in msgs are not acked; the broker redelivers them on next start.
	// Disconnect after writeLoop so that its last acks still get sent.
	client.Disconnect(1000)
	if err := archive.Close(); err != nil && writeErr == nil {
		writeErr = err
	}
	if writeErr == nil {
		log.Info("stopped")
	}
	return writeErr
}

// writeLoop appends received messages to the archive in batches, syncs and only then
// acks them, so a message is acked only once it is on disk.
func writeLoop(ctx context.Context, archive *Archive, msgs <-chan received, fatal <-chan error, log *slog.Logger) error {
	stats := time.NewTicker(statsInterval)
	defer stats.Stop()
	batch := make([]received, 0, maxBatch)
	count := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-fatal:
			return err
		case <-stats.C:
			log.Info("archived messages", "count", count, "period", statsInterval)
			count = 0
		case m := <-msgs:
			batch = append(batch[:0], m)
		drain:
			for len(batch) < maxBatch {
				select {
				case m := <-msgs:
					batch = append(batch, m)
				default:
					break drain
				}
			}
			for _, m := range batch {
				err := archive.Append(Record{
					ReceivedAt: m.at,
					Topic:      m.msg.Topic(),
					Payload:    m.msg.Payload(),
					Retained:   m.msg.Retained(),
				})
				if err != nil {
					return fmt.Errorf("append: %w", err)
				}
			}
			if err := archive.Sync(); err != nil {
				return fmt.Errorf("sync: %w", err)
			}
			for _, m := range batch {
				m.msg.Ack()
			}
			count += len(batch)
		}
	}
}

func mqttOptions(cfg config, log *slog.Logger, msgs chan<- received, done <-chan struct{}, fatal chan<- error) *mqtt.ClientOptions {
	handler := func(_ mqtt.Client, m mqtt.Message) {
		select {
		case msgs <- received{msg: m, at: time.Now()}:
		case <-done: // Shutting down; don't block paho, the message stays unacked
		}
	}
	return mqtt.NewClientOptions().
		AddBroker(cfg.broker).
		SetClientID(cfg.clientID).
		SetUsername(cfg.username).
		SetPassword(cfg.password).
		// Persistent session: the broker queues QoS 1 messages while we're away
		SetCleanSession(false).
		SetAutoAckDisabled(true).
		// Messages queued in the session arrive right after connect, before the
		// subscription is (re)made, so they need the default handler
		SetDefaultPublishHandler(handler).
		SetKeepAlive(30 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(time.Minute).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Warn("connection lost", "err", err)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Info("connected, subscribing", "topic", cfg.topic)
			t := c.Subscribe(cfg.topic, 1, handler)
			if !t.WaitTimeout(30*time.Second) || t.Error() != nil {
				// Reconnect will retry
				log.Error("subscribe failed", "err", t.Error())
				return
			}
			if qos := t.(*mqtt.SubscribeToken).Result()[cfg.topic]; qos > 2 {
				select {
				case fatal <- fmt.Errorf("broker rejected subscription to %s (check ACL)", cfg.topic):
				default:
				}
			}
		})
}

func configFromEnv() (config, error) {
	cfg := config{
		broker:   os.Getenv("MQTT_BROKER"),
		username: os.Getenv("MQTT_USERNAME"),
		password: os.Getenv("MQTT_PASSWORD"),
		clientID: envOr("MQTT_CLIENT_ID", "sensor-archiver"),
		topic:    envOr("MQTT_TOPIC", "/sensor/#"),
		dir:      envOr("ARCHIVE_DIR", "/data"),
	}
	if cfg.broker == "" {
		return cfg, fmt.Errorf("MQTT_BROKER is required (e.g. mqtts://mqtt.example.com:8883)")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
