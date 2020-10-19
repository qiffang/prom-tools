package main

import (
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"bufio"
	"fmt"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/labels"
	"io"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
)

func main() {
	var (
		cli            = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for importing data to prometheus")
		importCmd      = cli.Command("import", "run importtool")
		importDataPath = importCmd.Flag("input", "input file with samples data").String()
		writeOutPath   = importCmd.Flag("output", "set the output path").Default("benchout").String()
	)

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case importCmd.FullCommand():
		err := run(*writeOutPath, *importDataPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool

	storage *tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
	logger    log.Logger
}

func run(outPath, samplesFile string) error {
	b := &writeBenchmark{
		outPath:     outPath,
		samplesFile: samplesFile,
		logger:      log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
	}
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			return err
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		return err
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		return err
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.With(b.logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
		//RetentionDuration: int64(15 * 24 * time.Hour / time.Millisecond),
		//MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
	})
	if err != nil {
		return err
	}
	st.DisableCompactions()
	b.storage = st

	var series []Series

	if _, err = measureTime("readData", func() error {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			return err
		}
		defer f.Close()

		series, err = readPrometheusLabels(f)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	var total uint64

	dur, err := measureTime("ingestScrapes", func() error {
		if err := b.startProfiling(); err != nil {
			return err
		}
		total, err = b.ingestScrapes(series)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	if _, err = measureTime("stopStorage", func() error {
		if err := b.storage.Close(); err != nil {
			return err
		}

		return b.stopProfiling()
	}); err != nil {
		return err
	}

	return nil
}

func (b *writeBenchmark) ingestScrapes(series []Series) (uint64, error) {
	var mu sync.Mutex
	var total uint64

	var wg sync.WaitGroup
	ser := series
	for len(ser) > 0 {
		l := 1000
		if len(ser) < 1000 {
			l = len(ser)
		}
		batch := ser[:l]
		ser = ser[l:]

		wg.Add(1)
		go func() {
			n, err := b.ingestScrapesShard(batch)
			if err != nil {
				// exitWithError(err)
				fmt.Println(" err", err)
			}
			mu.Lock()
			total += n
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Println("ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(series []Series) (uint64, error) {
	//ts := baset

	type sample struct {
		labels    labels.Labels
		value     float64
		timestamp int64
		ref       *uint64
	}

	scrape := make([]*sample, 0, len(series))

	for _, s := range series {
		scrape = append(scrape, &sample{
			labels:    s.Mets,
			value:     s.Value,
			timestamp: s.Timestamp,
		})
	}
	total := uint64(0)

	app := b.storage.Appender()
	for _, s := range scrape {
		s.value += 1000

		if s.ref == nil {
			ref, err := app.Add(s.labels, s.timestamp, float64(s.value))
			if err != nil {
				panic(err)
			}
			s.ref = &ref
		} else if err := app.AddFast(*s.ref, s.timestamp, float64(s.value)); err != nil {

			if errors.Cause(err) != storage.ErrNotFound {
				panic(err)
			}

			ref, err := app.Add(s.labels, s.timestamp, float64(s.value))
			if err != nil {
				panic(err)
			}
			s.ref = &ref
		}

		total++
	}
	if err := app.Commit(); err != nil {
		return total, err
	}

	return total, nil
}

func measureTime(stage string, f func() error) (time.Duration, error) {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	if err := f(); err != nil {
		return 0, err
	}

	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start), nil
}

type Series struct {
	Mets      labels.Labels
	Value     float64
	Timestamp int64
}

func readPrometheusLabels(r io.Reader) ([]Series, error) {
	scanner := bufio.NewScanner(r)

	var series []Series
	hashes := map[uint64]struct{}{}

	for scanner.Scan() {
		m := make(labels.Labels, 0, 10)
		var ser Series

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())
		if strings.TrimSpace(s) == "" {
			break
		}

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			switch strings.TrimSpace(split[0]) {
			case "__value__":
				value, err := strconv.ParseFloat(strings.TrimSpace(split[1]), 64)
				if err != nil {
					logrus.Errorf("parse value failed, value=%s", strings.TrimSpace(split[1]))
					return nil, err
				}

				ser.Value = value
			case "__time__":
				timestamp, err := strconv.ParseInt(strings.TrimSpace(split[1]), 10, 64)
				if err != nil {
					logrus.Errorf("parse timestamp failed, value=%s", strings.TrimSpace(split[1]))
					return nil, err
				}

				ser.Timestamp = timestamp
			default:
				m = append(m, labels.Label{Name: split[0], Value: split[1]})
			}
		}

		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(m)
		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}

		ser.Mets = m
		series = append(series, ser)
		hashes[h] = struct{}{}
	}
	return series, nil
}

func (b *writeBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

func (b *writeBenchmark) startProfiling() error {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		return fmt.Errorf("bench: could not create cpu profile: %v", err)
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		return fmt.Errorf("bench: could not start CPU profile: %v", err)
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		return fmt.Errorf("bench: could not create memory profile: %v", err)
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		return fmt.Errorf("bench: could not create block profile: %v", err)
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		return fmt.Errorf("bench: could not create mutex profile: %v", err)
	}
	runtime.SetMutexProfileFraction(20)
	return nil
}
