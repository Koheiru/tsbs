package main

import (
	"bufio"
	"bytes"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

const errNotThreeTuplesFmt = "parse error: line does not have 3 tuples, has %d -> %s"

var newLine = []byte("\n")

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	return data.NewLoadedPoint(d.scanner.Bytes())
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

type batch struct {
	buf     *bytes.Buffer
	rows    uint
	metrics uint64
}

func (b *batch) Len() uint {
	return b.rows
}

func split(line string, delim rune) []string {
	output := make([]string, 0, 3)
	beginning := 0
	quoting := false
	escaping := false
	for index, symbol := range line {
		if symbol == '\\' {
			escaping = !escaping
			continue
		}

		if symbol == delim {
			if !escaping && !quoting {
				item := line[beginning:index]
				output = append(output, string(item))
				beginning = index + 1
			}
		} else if symbol == '"' {
			if !escaping {
				quoting = !quoting
			}
		}

		escaping = false
	}

	if beginning < len(line) {
		item := line[beginning:]
		output = append(output, string(item))
	}

	return output
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.([]byte)
	thatStr := string(that)
	b.rows++

	// Each influx line is format "csv-tags csv-fields timestamp", so we split by space
	// and then on the middle element, we split by comma to count number of fields added

	args := split(thatStr, ' ')
	if len(args) != 3 {
		fatal(errNotThreeTuplesFmt, len(args), thatStr)
		return
	}
	b.metrics += uint64(len(strings.Split(args[1], ",")))

	b.buf.Write(that)
	b.buf.Write(newLine)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
