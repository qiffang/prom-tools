package db

import (
	"context"
	"encoding/json"
	"github.com/oklog/ulid"
	"github.com/pingcap/errors"
	"github.com/prometheus/common/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/wal"
	"github.com/wushilin/stream"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"time"
)

const (
	metaName      = "meta.json"
	chunksName    = "chunks"
	indexName     = "index"
	tombstoneName = "tombstones"
	minBlockRange = int64(2*time.Hour) / 1e6
)

type promdb struct {
	dbpath string

	compactor *tsdb.LeveledCompactor
	start     int64
	end       int64
	blocks    []*block
	head      *tsdb.Head
}

type block struct {
	dir  string
	meta *tsdb.BlockMeta
}

func Open(dbpath string, startTimeInMilliSec int64, endTimeInMilliSec int64) (*promdb, error) {
	if dbpath == "" {
		return nil, errors.Errorf("empty prometheus data directory")
	}

	ctx, cancel := context.WithCancel(context.Background())
	compactor, err := tsdb.NewLeveledCompactor(ctx, nil, nil, tsdb.ExponentialBlockRanges(minBlockRange, 3, 5), chunkenc.NewPool())
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "create leveled compactor")
	}

	dirs, err := blockDirs(dbpath)
	if err != nil {
		return nil, errors.Trace(errors.Wrap(err, "find blocks fail"))
	}

	head, err := openHead(dbpath)
	if err != nil {
		return nil, errors.Wrap(err, "open head block fail")
	}

	minValidTime := int64(math.MinInt64)

	blocks := make([]*block, len(dirs))
	stream.FromArray(dirs).
		Map(func(dir string) *block {
			meta, _, err := readMetaFile(dir)
			if err != nil {
				panic(errors.Wrap(err, "read meta file failed"))
			}
			return &block{
				dir:  dir,
				meta: meta,
			}
		}).Peek(func(b *block) {
		minValidTime = max(minValidTime, b.meta.MaxTime)
	}).CollectTo(blocks)

	if initErr := head.Init(minValidTime); initErr != nil {
		return nil, errors.Wrap(err, "init head fail")
	}

	return &promdb{
		dbpath:    dbpath,
		start:     startTimeInMilliSec,
		end:       endTimeInMilliSec,
		compactor: compactor,
		blocks:    blocks,
		head:      head,
	}, nil
}

func (db *promdb) Dump(dumpdir string) error {
	if !Exists(dumpdir) {
		if err := os.Mkdir(dumpdir, os.ModePerm); err != nil {
			return errors.Wrap(err, "create dump directory failed")
		}
	}

	//snapdir := filepath.Join(db.dbpath, "dump")
	//name := fmt.Sprintf("%s-%x",
	//	time.Now().UTC().Format("20060102T150405Z0700"),
	//	rand.Int())
	//tmpdir := filepath.Join(snapdir, name)

	stream.FromArray(db.blocks).Filter(func(b *block) bool {
		if db.metaOverlap(b.meta) {
			return true
		}

		return false
	}).Each(func(b *block) {
		if err := db.link(b.meta.ULID.String(), b.dir, dumpdir); err != nil {
			panic(errors.Wrap(err, "link block fail"))
		}
	})

	if db.overlap(db.head.MinTime(), db.head.MaxTime()) {
		if err := db.dumpHead(dumpdir); err != nil {
			return err
		}
	}

	return nil
}

func openHead(dbpath string) (*tsdb.Head, error) {
	wlog, err := wal.NewSize(nil, nil, filepath.Join(dbpath, "wal"), wal.DefaultSegmentSize)
	if err != nil {
		log.Error("open wal failed", err)
		return nil, errors.Trace(err)
	}

	head, err := tsdb.NewHead(nil, nil, wlog, 1)
	if err != nil {
		log.Error("init head chunk failed", err)
		return nil, errors.Trace(err)
	}

	return head, nil
}

func max(v1 int64, v2 int64) int64 {
	if v1 > v2 {
		return v1
	}

	return v2
}

func (db *promdb) metaOverlap(meta *tsdb.BlockMeta) bool {
	return db.overlap(meta.MinTime, meta.MaxTime)
}

func (db *promdb) overlap(min int64, max int64) bool {
	return !(db.end < min || db.start > max)
}

func readMetaFile(dir string) (*tsdb.BlockMeta, int64, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, metaName))
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	var m tsdb.BlockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, 0, errors.Trace(err)
	}
	if m.Version != 1 {
		return nil, 0, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return &m, int64(len(b)), nil
}

func (db *promdb) link(metaID string, dir, dumpdir string) error {
	blockDir := filepath.Join(dumpdir, metaID)
	if err := os.MkdirAll(blockDir, 0777); err != nil {
		return errors.Wrap(err, "create dump block dir")
	}

	chunksDir := chunkDir(blockDir)
	if err := os.MkdirAll(chunksDir, 0777); err != nil {
		return errors.Wrap(err, "create dump chunk dir")
	}

	// Hardlink meta, index and tombstones
	for _, fname := range []string{
		metaName,
		indexName,
		tombstoneName,
	} {
		if err := os.Link(filepath.Join(dir, fname), filepath.Join(blockDir, fname)); err != nil {
			return errors.Wrapf(err, "create dump %s", fname)
		}
	}

	// Hardlink the chunks
	curChunkDir := chunkDir(dir)
	files, err := ioutil.ReadDir(curChunkDir)
	if err != nil {
		return errors.Wrap(err, "ReadDir the current chunk dir")
	}

	for _, f := range files {
		err := os.Link(filepath.Join(curChunkDir, f.Name()), filepath.Join(chunksDir, f.Name()))
		if err != nil {
			return errors.Wrap(err, "hardlink a chunk")
		}
	}

	return nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func chunkDir(dir string) string { return filepath.Join(dir, chunksName) }

func (db *promdb) dumpHead(dumpdir string) error {
	_, err := db.compactor.Write(dumpdir, db.head, db.head.MinTime(), db.head.MaxTime(), nil)
	return errors.Wrap(err, "dump head block")
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
