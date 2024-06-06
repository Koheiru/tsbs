package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/influx"
)

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget
)

type telegrafTarget struct {
}

func (t *telegrafTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
}

func (t *telegrafTarget) TargetName() string {
	return constants.FormatInflux
}

func (t *telegrafTarget) Serializer() serialize.PointSerializer {
	return &influx.Serializer{}
}

func (t *telegrafTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}

// allows for testing
var fatal = log.Fatalf

// Program option vars:
var (
	telegrafURLs   []string
	useGzip        bool
	backoff        time.Duration
	consistency    string
	doAbortOnExist bool
)

// Parse args:
func init() {
	target = &telegrafTarget{}
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	telegrafURLs = strings.Split(viper.GetString("urls"), ",")
	if len(telegrafURLs) == 0 {
		log.Fatal("missing 'urls' flag")
	}

	useGzip = viper.GetBool("gzip")
	backoff = viper.GetDuration("backoff")
	consistency = "any"

	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

/*
type DBCreator interface {
	// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
	Init()

	// DBExists checks if a database with the given name currently exists.
	DBExists(dbName string) bool

	// CreateDB creates a database with the given name.
	CreateDB(dbName string) error

	// RemoveOldDB removes an existing database with the given name.
	RemoveOldDB(dbName string) error
}
*/

type dbCreator struct{}

func (d *dbCreator) Init() {
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(config.FileName))}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	loader.RunBenchmark(&benchmark{})
}
