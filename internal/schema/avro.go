package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"search_trend/internal/model"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

const confluentMagicByte byte = 0

type AvroCodec struct {
	schemaID int
	codec    *goavro.Codec
}

func NewAvroCodec(schemaID int, schemaText string) (*AvroCodec, error) {
	codec, err := goavro.NewCodec(schemaText)
	if err != nil {
		return nil, fmt.Errorf("create avro codec: %w", err)
	}

	return &AvroCodec{
		schemaID: schemaID,
		codec:    codec,
	}, nil
}

func (c *AvroCodec) Encode(event model.SearchEvent) ([]byte, error) {
	native := map[string]interface{}{
		"event_id":  event.EventID.String(),
		"query":     event.Query,
		"user_id":   nullableString(uuidString(event.UserID)),
		"ip_hash":   nullableString(event.IPHash),
		"timestamp": event.Timestamp.UnixMilli(),
		"source":    event.Source,
	}
	if native["source"] == "" {
		native["source"] = "UNKNOWN"
	}

	binaryPayload, err := c.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("encode avro event: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteByte(confluentMagicByte)
	if err := binary.Write(&buf, binary.BigEndian, int32(c.schemaID)); err != nil {
		return nil, err
	}
	buf.Write(binaryPayload)

	return buf.Bytes(), nil
}

func uuidString(value *uuid.UUID) string {
	if value == nil {
		return ""
	}

	return value.String()
}

func (c *AvroCodec) Decode(data []byte) (model.SearchEvent, error) {
	if len(data) < 5 {
		return model.SearchEvent{}, fmt.Errorf("avro payload too short")
	}
	if data[0] != confluentMagicByte {
		return model.SearchEvent{}, fmt.Errorf("invalid confluent magic byte")
	}

	native, _, err := c.codec.NativeFromBinary(data[5:])
	if err != nil {
		return model.SearchEvent{}, fmt.Errorf("decode avro event: %w", err)
	}

	record, ok := native.(map[string]interface{})
	if !ok {
		return model.SearchEvent{}, fmt.Errorf("unexpected avro native type %T", native)
	}

	eventID, err := uuid.Parse(requiredString(record, "event_id"))
	if err != nil {
		return model.SearchEvent{}, fmt.Errorf("parse event_id: %w", err)
	}

	var userID *uuid.UUID
	if rawUserID := unionString(record["user_id"]); rawUserID != "" {
		parsed, err := uuid.Parse(rawUserID)
		if err != nil {
			return model.SearchEvent{}, fmt.Errorf("parse user_id: %w", err)
		}
		userID = &parsed
	}

	return model.SearchEvent{
		EventID:   eventID,
		Query:     requiredString(record, "query"),
		UserID:    userID,
		IPHash:    unionString(record["ip_hash"]),
		Timestamp: time.UnixMilli(requiredInt64(record, "timestamp")).UTC(),
		Source:    requiredString(record, "source"),
	}, nil
}

func IsConfluentAvro(data []byte) bool {
	return len(data) >= 5 && data[0] == confluentMagicByte
}

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}

	return map[string]interface{}{
		"string": value,
	}
}

func requiredString(record map[string]interface{}, key string) string {
	value, _ := record[key].(string)
	return value
}

func unionString(value interface{}) string {
	if value == nil {
		return ""
	}
	if raw, ok := value.(string); ok {
		return raw
	}
	if union, ok := value.(map[string]interface{}); ok {
		if raw, ok := union["string"].(string); ok {
			return raw
		}
	}

	return ""
}

func requiredInt64(record map[string]interface{}, key string) int64 {
	switch value := record[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case float64:
		return int64(value)
	case time.Time:
		return value.UnixMilli()
	default:
		return 0
	}
}
