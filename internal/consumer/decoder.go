package consumer

import (
	"encoding/json"
	"fmt"
	"search_trend/internal/schema"
	"time"

	"search_trend/internal/model"
)

const (
	maxAge        = 10 * time.Minute
	maxFutureSkew = 30 * time.Second
)

type SearchEventDecoder struct {
	avroCodec *schema.AvroCodec
}

func NewJSONDecoder() *SearchEventDecoder {
	return &SearchEventDecoder{}
}

func NewDecoder(avroCodec *schema.AvroCodec) *SearchEventDecoder {
	return &SearchEventDecoder{
		avroCodec: avroCodec,
	}
}

func (d *SearchEventDecoder) Decode(data []byte) (model.SearchEvent, error) {
	if d.avroCodec != nil && schema.IsConfluentAvro(data) {
		event, err := d.avroCodec.Decode(data)
		if err != nil {
			return model.SearchEvent{}, err
		}

		if err := event.Validate(time.Now().UTC(), maxAge, maxFutureSkew); err != nil {
			return model.SearchEvent{}, err
		}

		return event, nil
	}

	var event model.SearchEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return model.SearchEvent{}, fmt.Errorf("decode search event: %w", err)
	}

	if err := event.Validate(time.Now().UTC(), maxAge, maxFutureSkew); err != nil {
		return model.SearchEvent{}, err
	}

	return event, nil
}
