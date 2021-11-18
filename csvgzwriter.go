package csvgz

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/jszwec/csvutil"
)

type CsvGzWriter struct {
	OutputWriter io.WriteCloser
	CsvWriter    *csvutil.Encoder
	FirstRow     interface{}
	LastRow      interface{}
	TotalRows    int64
	csvWriter    *csv.Writer
	gzipWriter   *gzip.Writer
}
type SqlRows interface {
	Next() bool
}

func New(output io.WriteCloser, comma ...rune) (*CsvGzWriter, error) {
	c := CsvGzWriter{}
	c.TotalRows = 0
	c.FirstRow = nil
	c.LastRow = nil
	c.gzipWriter = gzip.NewWriter(output)
	c.csvWriter = csv.NewWriter(c.gzipWriter)
	if len(comma) == 1 {
		c.csvWriter.Comma = comma[0]
	}
	c.CsvWriter = csvutil.NewEncoder(c.csvWriter)
	c.OutputWriter = output
	return &c, nil
}
func (c *CsvGzWriter) Header(header []string) (err error) {
	c.CsvWriter.AutoHeader = false
	return c.csvWriter.Write(header)
}

func (c *CsvGzWriter) HeaderWithStruct(v interface{}, tag string) (err error) {
	c.CsvWriter.AutoHeader = false
	s, err := csvutil.Header(v, tag)
	if err != nil {
		return err
	}
	return c.csvWriter.Write(s)
}
func (c *CsvGzWriter) MarshalStuct(v interface{}) (err error) {
	if c.FirstRow == nil {
		c.FirstRow = v
	}
	c.LastRow = v
	c.TotalRows++
	err = c.CsvWriter.Encode(v)
	return
}

func (c *CsvGzWriter) MarshalStuctSlice(a interface{}) (err error) {
	v, ok := a.([]interface{})
	if ok {
		return fmt.Errorf("Error argument is not StructSlice.")
	}
	c.TotalRows += int64(len(v))
	if c.TotalRows > 0 {
		if c.FirstRow == nil {
			c.FirstRow = v[0]
		}
		c.LastRow = v[len(v)-1]
	}
	err = c.CsvWriter.Encode(v)
	c.Flush()
	return
}

func (c *CsvGzWriter) Flush() {
	c.csvWriter.Flush()
	c.gzipWriter.Flush()
}

func (c *CsvGzWriter) Close() {
	c.Flush()
	_ = c.gzipWriter.Close()
	_ = c.OutputWriter.Close()
}
