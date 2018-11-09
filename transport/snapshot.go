package transport

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrSnapshotIterStop error = errors.New("snapshot: iterator stop")
)

type SnapshotMetadata struct {
	StateMachineMeta []byte `json:"state_machine_meta"`
	TopologyMeta     []byte `json:"topology_meta"`
}

func (s *SnapshotMetadata) Marshal() (p []byte, err error) {
	// FIXME: Protobuf
	p, err = json.Marshal(s)
	return
}

func (s *SnapshotMetadata) Unmarshal(p []byte) (err error) {
	err = json.Unmarshal(p, s)
	return
}

type Snapshot interface {
	GetID() string
	Meta() map[string]string
}

type SnapshotReader interface {
	Snapshot
	SnapshotIterator
	Marshaler

	// FIXME: Commit ?
	Commit() error
}

type SnapshotIterator interface {
	Next() <-chan SnapshotParter
}

type SnapshotParter interface {
	CreateReader() (CloseReader, error)
	CreateWriter() (CloseWriter, error)
	Marshaler
	Meta() map[string]string
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Closer interface {
	Close() error
}

type CloseReader interface {
	Read(p []byte) (n int, err error)
	Closer
}

type CloseWriter interface {
	Write(p []byte) (n int, err error)
	Closer
}

// A simple file snapshot reader
var _ SnapshotReader = (*SnapshotFileReader)(nil)

type SnapshotFileReader struct {
	ID          string            `json:"id"`
	Metadata    map[string]string `json:"metadata"`
	Direcotries []*Directory      `json:"direcotries"`

	TmpDir string `json:"-"`
	Dir    string `json:"-"`
}

func (s *SnapshotFileReader) GetID() string { return s.ID }

func (s *SnapshotFileReader) Meta() map[string]string {
	return s.Metadata
}

func (s *SnapshotFileReader) Marshal() (p []byte, err error) {
	p, err = json.Marshal(s)
	return
}

func (s *SnapshotFileReader) files() (files []*File) {
	for _, dir := range s.Direcotries {
		files = append(files, dir.Files...)
	}
	return
}

func (s *SnapshotFileReader) Next() <-chan SnapshotParter {
	// closed iterator
	iterc := make(chan SnapshotParter, len(s.files()))
	for _, dir := range s.Direcotries {
		for _, file := range dir.Files {
			iterc <- file
		}
	}
	close(iterc)
	return iterc
}

func (s *SnapshotFileReader) Commit() (err error) {
	// FIXME: How to clean up the directories if crashed.

	for _, dir := range s.Direcotries {
		fullDir := path.Join(s.Dir, dir.Dir)
		// move the old one
		backupDir := fmt.Sprintf("%s-backup-%s", fullDir, time.Now().Format(time.RFC1123))
		log.Printf("[SnapshotFileReader] original directory: %s rename to %s\n", fullDir, backupDir)
		if err = os.Rename(fullDir, backupDir); err != nil {
			return errors.Wrapf(err, "[SnapshotFileReader] rename %s to %s failed\n", fullDir, backupDir)
		}

		tmpDir := path.Join(s.TmpDir, dir.Dir)
		log.Printf("[SnapshotFileReader] tmp directory: %s rename to target %s\n", tmpDir, fullDir)
		if err = os.Rename(tmpDir, fullDir); err != nil {
			return errors.Wrapf(err, "[SnapshotFileReader] rename %s to %s failed", tmpDir, fullDir)
		}
		os.RemoveAll(tmpDir)
	}

	return nil
}

type Directory struct {
	Dir string `json:"dir"` // examples: /blob or /meta

	Files []*File `json:"files"`
}

type File struct {
	Filename string            `json:"filename"` // This is a part file path after volume directory
	Size     int64             `json:"size"`
	MD5      []byte            `json:"md5"`
	Metadata map[string]string `json:"metadata"`

	Dir    string `json:"-"` // This directory is different between leader and peers
	TmpDir string `json:"-"`
}

func (f *File) CreateReader() (CloseReader, error) {
	// FIXME: User has the responsibility to close the file handle
	fh, err := os.OpenFile(f.FullFilename(), os.O_RDONLY, 0755)
	if err != nil {
		return nil, errors.Wrapf(err, "[File] open file(%s) failed. %v", f.Filename, err)
	}
	return fh, nil
}

func (f *File) CreateWriter() (CloseWriter, error) {
	// FIXME: User has the responsibility to close the file handle
	fh, err := os.OpenFile(f.TmpFilename(), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return nil, errors.Wrapf(err, "[File] open file(%s) failed. %v", f.TmpFilename(), err)
	}
	return fh, nil
}

func (f *File) Marshal() (p []byte, err error) {
	p, err = json.Marshal(f)
	return
}

func (f *File) Meta() map[string]string {
	return f.Metadata
}

func (f *File) TmpFilename() string {
	return path.Join(f.TmpDir, f.Filename)
}

func (f *File) FullFilename() string {
	return path.Join(f.Dir, f.Filename)
}
