package db

import (
	"context"
	"encoding/json"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/wal"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	metaName = "meta.json"
	chunksName = "chunks"
    indexName = "index"
	tombstoneName = "tombstones"
	minBlockRange = int64(2*time.Hour)/1e6
)

type promdb struct {
	dbpath string

	compactor *tsdb.LeveledCompactor
	start int64
	end int64
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

	return &promdb{
		dbpath: dbpath,
		start: startTimeInMilliSec,
		end: endTimeInMilliSec,
		compactor: compactor,
	}, nil
}

func (db *promdb) Dump(dumpdir string) error {
	if dumpdir == db.dbpath {
		return errors.Errorf("cannot dump into base directory")
	}

	dirs, err := blockDirs(db.dbpath)
	if err != nil {
		log.Warn("find blocks failed", err)
		return err
	}

	for _, dir := range dirs {
		meta, _, err := db.readMetaFile(dir)
		if err != nil {
			log.Warn("read meta file failed", "dir=" + dir, err)
			continue
		}

		if db.overlap(meta) {
			db.link(meta.ULID.String(), dir, dumpdir)
		}
	}

	return nil
}

func (db *promdb) overlap(meta *tsdb.BlockMeta) bool {
	return !(db.end < meta.MinTime || db.start > meta.MaxTime)
}

func (db *promdb) readMetaFile(dir string) (*tsdb.BlockMeta, int64, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, metaName))
	if err != nil {
		return nil, 0, err
	}
	var m tsdb.BlockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, 0, err
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

func (db *promdb) head(dir string) error{
	wlog, err := wal.NewSize(nil, nil, filepath.Join(dir, "wal"), wal.DefaultSegmentSize)
	if err != nil {
		log.Error("open wal failed", err)
		return err
	}

	head, err := tsdb.NewHead(nil, nil, wlog, minBlockRange)
	if err != nil {
		log.Error("init head chunk failed", err)
		return err
	}


	_, err = db.compactor.Write(dir, head, db.start, db.end, nil)
	return errors.Wrap(err, "dump head block")
}


