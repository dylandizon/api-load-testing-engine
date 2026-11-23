package dataset

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"
)

// Dataset provides a streaming, row-by-row iterator over a tabular dataset.
type Dataset struct {
	mu      sync.Mutex
	reader  *csv.Reader
	headers []string
	rewind  func() (io.Reader, error)
}

// NewStreamingCSV creates a streaming dataset from an io.Reader.
func NewStreamingCSV(r io.Reader) (*Dataset, error) {
	csvReader := csv.NewReader(r)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read dataset headers: %w", err)
	}

	return &Dataset{
		reader:  csvReader,
		headers: headers,
	}, nil
}

// Next reads the next row in the dataset, returning key-value pairs matching header names.
// When EOF is reached, it returns io.EOF.
func (d *Dataset) Next() (map[string]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	record, err := d.reader.Read()
	if err != nil {
		return nil, err
	}

	row := make(map[string]string, len(d.headers))
	for i, header := range d.headers {
		if i < len(record) {
			row[header] = record[i]
		}
	}
	return row, nil
}

// Headers returns the column names of the dataset.
func (d *Dataset) Headers() []string {
	return d.headers
}
