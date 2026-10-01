# sensor-archiver

Subscribes to MQTT and writes every received message to daily, zstd-compressed
JSON Lines files. Meant for long-term storage of sensor data that is analysed offline
(e.g. with DuckDB), replacing InfluxDB.

## How it works

- Persistent MQTT session (`clean session = false`, fixed client ID, QoS 1), so the
  broker queues messages while the archiver is down.
- Messages are acked only after they have been written and fsynced. A crash or restart
  never loses a message, but may archive a few twice (at-least-once).
- The current day is written to `<dir>/YYYY/YYYY-MM-DD.jsonl`. After UTC midnight the
  file is compressed to `YYYY-MM-DD.jsonl.zst` and the raw file removed. Raw files left
  by a previous run are compressed on startup, and a partially written last line is
  dropped.
- Existing `.zst` files are never overwritten; a numbered name
  (`YYYY-MM-DD.1.jsonl.zst`) is used instead.
- If the clock goes backwards, records keep going to the newest file, so a file may
  contain a few records from a neighbouring day.

## Record format

One JSON object per line:

```json
{"received_at":"2026-10-01T12:34:56.789Z","topic":"/sensor/A/t/state","payload":{"temperature":21.5,"ts":"..."}}
```

| Field            | Description                                                                                                                                                     |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `received_at`    | UTC time the archiver received the message. Messages queued during downtime get the time they were delivered, not published, so use the payload's own `ts` when it has one. |
| `topic`          | MQTT topic.                                                                                                                                                     |
| `retained`       | `true` for retained messages that the broker sends on every (re)subscribe. These repeat the last known state and should usually be filtered out.                |
| `payload`        | The payload as JSON if it is valid JSON, otherwise as a string.                                                                                                 |
| `payload_base64` | Used instead of `payload` if the payload isn't valid UTF-8.                                                                                                     |

## Configuration

| Variable         | Default           | Description                                              |
|------------------|-------------------|----------------------------------------------------------|
| `MQTT_BROKER`    | (required)        | e.g. `tcp://localhost:1883` or `mqtts://host:8883`       |
| `MQTT_USERNAME`  |                   |                                                          |
| `MQTT_PASSWORD`  |                   |                                                          |
| `MQTT_CLIENT_ID` | `sensor-archiver` | Identifies the persistent session. Keep it stable.       |
| `MQTT_TOPIC`     | `/sensor/#`       | Subscription filter                                      |
| `ARCHIVE_DIR`    | `/data`           |                                                          |

The image runs as uid 65532, which needs write access to the archive directory.

Broker settings that matter for not losing data:

- Publishers must use QoS 1. QoS 0 messages are not queued for offline clients unless
  Mosquitto has `queue_qos0_messages true`.
- `persistent_client_expiration` is how long the archiver can be down before its
  session (and the queued messages) is dropped.
- `max_queued_messages` limits how many messages are queued meanwhile.

## Analysing with DuckDB

```sql
SELECT received_at, topic, payload->>'instance' AS instance, (payload->>'temperature')::DOUBLE AS temperature
FROM read_json('archive/*/*.jsonl.zst',
               format = 'newline_delimited',
               columns = {received_at: 'TIMESTAMPTZ', topic: 'VARCHAR', retained: 'BOOLEAN', payload: 'JSON'})
WHERE NOT coalesce(retained, false)
  AND topic LIKE '/sensor/%/t/state';
```

To include today's uncompressed file, add `'archive/*/*.jsonl'` to the file list.

## Development

```sh
go test ./...
docker build -t sensor-archiver .
```

Pushes to `main` build and push `ghcr.io/chacal/sensor-archiver` (amd64 and arm64).
