package ksqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

type WithValueFormat string

const (
	ValueFormatAvro      = "'AVRO'"
	ValueFormatJson      = "'JSON'"
	ValueFormatDelimited = "'DELIMITED'"

	WithPropertyKafkaTopic  = "KAFKA_TOPIC"
	WithPropertyValueFormat = "VALUE_FORMAT"
	WithPropertyKey         = "KEY"
	WithPropertyTimeStamp   = "TIMESTAMP"
	WithPropertyPartitions  = "PARTITIONS"
	WithPropertyReplicas    = "REPLICAS"
)

var withProperties = []string{
	WithPropertyKafkaTopic,
	WithPropertyValueFormat,
	WithPropertyKey,
	WithPropertyTimeStamp,
	WithPropertyPartitions,
	WithPropertyReplicas,
}

type with struct {
	KafkaTopic  string
	ValueFormat WithValueFormat
	Partitions  int
	Replicas    int
	TimeStamp   string
	Key         string
}

func (w *with) String() string {
	var sb []string

	sb = append(sb, fmt.Sprintf("%s %s %s", WithPropertyKafkaTopic, ReservedEq, w.KafkaTopic))

	if string(w.ValueFormat) != "" {
		sb = append(sb, fmt.Sprintf("%s%s %s %s", ReservedComma, WithPropertyValueFormat, ReservedEq, w.ValueFormat))
	}
	if w.Key != "" {
		sb = append(sb, fmt.Sprintf("%s%s %s %s", ReservedComma, WithPropertyKey, ReservedEq, w.Key))
	}
	if w.TimeStamp != "" {
		sb = append(sb, fmt.Sprintf("%s%s %s %s", ReservedComma, WithPropertyTimeStamp, ReservedEq, w.TimeStamp))
	}
	if w.Replicas > 0 {
		sb = append(sb, fmt.Sprintf("%s%s %s %s", ReservedComma, WithPropertyReplicas, ReservedEq, strconv.Itoa(w.Replicas)))
	}
	if w.Partitions > 0 {
		sb = append(sb, fmt.Sprintf("%s%s %s %s", ReservedComma, WithPropertyPartitions, ReservedEq, strconv.Itoa(w.Partitions)))
	}
	return strings.Join(sb, " ")
}

func (p *parser) parseWith(withProperties ...string) (*with, error) {
	result := with{}
	for {
		prop, err := p.popOrError(withProperties...)
		if err != nil {
			return nil, err
		}
		if _, err := p.popOrError(ReservedEq); err != nil {
			return nil, err
		}

		switch prop {
		case WithPropertyKafkaTopic:
			topic, l := p.peekQuotedStringWithLength()
			if l == 0 {
				return nil, p.Error("'topic name'")
			}
			p.popLength(l)
			result.KafkaTopic = topic
		case WithPropertyKey:
			key, l := p.peekQuotedStringWithLength()
			if l == 0 {
				return nil, p.Error("'key'")
			}
			p.popLength(l)
			result.Key = key
		case WithPropertyTimeStamp:
			timestamp, l := p.peekQuotedStringWithLength()
			if l == 0 {
				return nil, p.Error("'timestamp'")
			}
			p.popLength(l)
			result.TimeStamp = timestamp
		case WithPropertyValueFormat:
			propertyValueFormat, err := p.popOrError(ValueFormatAvro, ValueFormatDelimited, ValueFormatJson)
			if err != nil {
				return nil, err
			}
			result.ValueFormat = WithValueFormat(propertyValueFormat)
		case WithPropertyPartitions:
			n := p.pop()
			i, err := strconv.Atoi(n)
			if err != nil {
				return nil, p.Error(DataTypeInt)
			}
			result.Partitions = i
		case WithPropertyReplicas:
			n := p.pop()
			i, err := strconv.Atoi(n)
			if err != nil {
				return nil, p.Error(DataTypeInt)
			}
			result.Replicas = i
		}

		next, err := p.popOrError(ReservedComma, ReservedCloseParens)
		if err != nil {
			return nil, err
		}
		switch next {
		case ReservedComma:
			continue
		case ReservedCloseParens:
			return &result, nil
		}
	}
}
