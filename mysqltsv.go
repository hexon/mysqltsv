// Package mysqltsv encodes values for usage in LOAD DATA INFILE's tab separated values.
package mysqltsv

// TODO: Heap escape analyses

import (
	"bufio"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

// Escaping explains the escaping this package uses for inclusion in a LOAD DATA INFILE statement.
const Escaping = `CHARACTER SET binary FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' STARTING BY ''`

/*
type Options struct {
	FieldsTerminatedBy       string
	FieldsEnclosedBy         string
	FieldsOptionallyEnclosed bool
	FieldsEscapedBy          string
	LinesTerminedBy          string
	LinesStartingBy          string

	// Character set?
}

func DefaultOptions() Options {
	return Options{
		FieldsTerminatedBy: "\t",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesTerminedBy:    "\n",
	}
}
*/

// EncoderOptions are settings that affect encoding.
type EncoderOptions struct {
	// Location is the timezone each time.Time will be converted to before being serialized.
	Location *time.Location
}

// Encoder encodes values into a CSV file suitable for consumption by LOAD DATA INFILE.
// The number of columns per row must be fixed, and it will automatically advance to the next row once all columns were appended.
// Any errors during appending will be stored and future calls will be ignored.
// The encoder must be Close()d once done to flush and to read any errors that might have occurred.
type Encoder struct {
	w                *bufio.Writer
	numColumnsPerRow int
	colsLeftInRow    int
	err              error
	encoderOptions   *EncoderOptions
}

// NewEncoder starts a new encoder. You should write the same number of columns per line and the Encoder will decide when a row is finished.
// Close must be called to see if any error occurred.
// EncoderOptions is optional.
func NewEncoder(w io.Writer, numColumns int, cfg *EncoderOptions) *Encoder {
	return &Encoder{
		w:                bufio.NewWriter(w),
		numColumnsPerRow: numColumns,
		colsLeftInRow:    numColumns,
		encoderOptions:   cfg,
	}
}

func (e *Encoder) writeField(b []byte) {
	buf := e.w.AvailableBuffer()
	_, e.err = e.w.Write(escapeField(buf, b))
	if e.err != nil {
		return
	}
	e.colsLeftInRow--
	if e.colsLeftInRow == 0 {
		e.err = e.w.WriteByte('\n')
		e.colsLeftInRow = e.numColumnsPerRow
	} else {
		e.err = e.w.WriteByte('\t')
	}
}

func (e *Encoder) AppendString(s string) {
	e.AppendBytes([]byte(s))
}

func (e *Encoder) AppendBytes(b []byte) {
	if e.err != nil {
		return
	}
	e.writeField(b)
}

func (e *Encoder) AppendValue(v any) {
	if e.err != nil {
		return
	}
	b, err := valueToBytes(v, e.encoderOptions)
	if err != nil {
		e.err = err
		return
	}
	e.writeField(b)
}

func (e *Encoder) Close() error {
	if e.err != nil {
		return e.err
	}
	if err := e.w.Flush(); err != nil {
		return err
	}
	return nil
}

func (e *Encoder) Error() error {
	return e.err
}

// Per https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling
func escapeField(appendTo, data []byte) []byte {
	if data == nil {
		return []byte{'\\', 'N'}
	}
	if cap(appendTo) < len(data)+2 {
		appendTo = make([]byte, 0, len(data)+5)
	}
	appendTo = append(appendTo, '"')
	for _, c := range data {
		switch c {
		case 0:
			appendTo = append(appendTo, '\\', '0')
		case '\b':
			appendTo = append(appendTo, '\\', 'b')
		case '\n':
			appendTo = append(appendTo, '\\', 'n')
		case '\r':
			appendTo = append(appendTo, '\\', 'r')
		case '\t':
			appendTo = append(appendTo, '\\', 't')
		case 26:
			appendTo = append(appendTo, '\\', 'Z')
		case '\\':
			appendTo = append(appendTo, '\\', '\\')
		case '"':
			appendTo = append(appendTo, '\\', '"')
		default:
			appendTo = append(appendTo, c)
		}
	}
	appendTo = append(appendTo, '"')
	return appendTo
}

func valueToBytes(v any, cfg *EncoderOptions) ([]byte, error) {
	if dv, ok := v.(driver.Valuer); ok {
		var err error
		v, err = dv.Value()
		if err != nil {
			return nil, err
		}
	}
	switch v := v.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case json.RawMessage:
		return v, nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case nil:
		return nil, nil
	case bool:
		if v {
			return []byte{'1'}, nil
		}
		return []byte{'0'}, nil
	case time.Time:
		if cfg != nil && cfg.Location != nil {
			v = v.In(cfg.Location)
		}
		hour, min, sec := v.Clock()
		nsec := v.Nanosecond()
		if hour == 0 && min == 0 && sec == 0 && nsec == 0 {
			return []byte(v.Format("2006-01-02")), nil
		}
		if nsec == 0 {
			return []byte(v.Format("2006-01-02 15:04:05")), nil
		}
		return []byte(v.Format("2006-01-02 15:04:05.999999999")), nil
	default:
		return nil, fmt.Errorf("can't encode type %T to TSV", v)
	}
}

// EscapeValue escapes a value for use in a MySQL CSV. It's escaped as shown in the constant Escaping.
// EncoderOptions is optional.
func EscapeValue(v any, cfg *EncoderOptions) ([]byte, error) {
	b, err := valueToBytes(v, cfg)
	if err != nil {
		return nil, err
	}
	return escapeField(nil, b), nil
}
