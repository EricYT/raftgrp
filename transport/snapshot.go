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

const (
	snapDirPerm  = 0755
	snapFilePerm = 0644
)

type SnapshotMetadata struct {
	UserStateMachineMeta []byte `json:"state_machine_meta"`
	TopologyMeta         []byte `json:"topology_meta"`
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
	Metadata() map[string]string
}

type SnapshotReader interface {
	Snapshot
	SnapshotIterator
	Marshaler
}

type SnapshotWriter interface {
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
	SnapshotPartReader
	SnapshotPartWriter

	Marshaler
	Metadata() map[string]string
}

type SnapshotPartReader interface {
	OpenReader() (CloseReader, error)
}

type SnapshotPartWriter interface {
	OpenWriter() (CloseWriter, error)
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal([]byte) error
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
var _ SnapshotWriter = (*SnapshotFileWriter)(nil)

type SnapshotFileBase struct {
	ID          string            `json:"id"`
	Meta        map[string]string `json:"meta"`
	Directories []*Directory      `json:"direcotries"`

	Dir string `json:"-"`
}

func NewSnapshotFileBase() *SnapshotFileBase {
	return &SnapshotFileBase{
		Meta: make(map[string]string),
	}
}

func (s *SnapshotFileBase) GetID() string {
	return s.ID
}

func (s *SnapshotFileBase) Metadata() map[string]string {
	return s.Meta
}

func (s *SnapshotFileBase) SetDir(dir string) {
	s.Dir = dir
	for _, file := range s.files() {
		file.SetParentDir(dir)
	}
}

func (s *SnapshotFileBase) EnsureDirectories() error {
	for _, dir := range s.Directories {
		dirpath := path.Join(s.Dir, dir.Dir)
		if err := os.MkdirAll(dirpath, snapDirPerm); err != nil {
			log.Printf("[SnapshotFileBase] create snapshot file base dir:(%s) failed. %v\n", dirpath, err)
			return err
		}
	}
	return nil
}

func (s *SnapshotFileBase) Marshal() (p []byte, err error) {
	p, err = json.Marshal(s)
	return
}

// FIXME: put this method inside SnapshotFileBase so
// it's easy to change the marshal and unmarshal methods.
func (s *SnapshotFileBase) Unmarshal(p []byte) (err error) {
	err = json.Unmarshal(p, s)
	return
}

func (s *SnapshotFileBase) files() (files []*File) {
	for _, dir := range s.Directories {
		files = append(files, dir.Files...)
	}
	return
}

func (s *SnapshotFileBase) Next() <-chan SnapshotParter {
	// closed iterator
	iterc := make(chan SnapshotParter, len(s.files()))
	for _, dir := range s.Directories {
		for _, file := range dir.Files {
			iterc <- file
		}
	}
	close(iterc)
	return iterc
}

// snapshot file reader
type SnapshotFileReader struct {
	*SnapshotFileBase
}

func NewSnapshotFileReader(b *SnapshotFileBase) *SnapshotFileReader {
	return &SnapshotFileReader{
		SnapshotFileBase: b,
	}
}

// snapshot file writer
type SnapshotFileWriter struct {
	*SnapshotFileBase
	TmpDir    string `json:"-"`
	TargetDir string `json:"target_dir"`
}

func NewSnapshotFileWriter(b *SnapshotFileBase) *SnapshotFileWriter {
	return &SnapshotFileWriter{
		SnapshotFileBase: b,
	}
}

func (s *SnapshotFileWriter) SetTmpDir(tmp string) {
	s.TmpDir = tmp
	s.SetDir(tmp)
}

func (s *SnapshotFileWriter) SetTargetDir(target string) {
	s.TargetDir = target
}

func (s *SnapshotFileWriter) EnsureTmpDirectories() error {
	return s.SnapshotFileBase.EnsureDirectories()
}

func (s *SnapshotFileWriter) Commit() (err error) {
	// FIXME: How to clean up the directories if crashed.

	backupSuffix := time.Now().Nanosecond()
	for _, dir := range s.Directories {
		targetDir := path.Join(s.TargetDir, dir.Dir)
		// move the old one
		backupDir := fmt.Sprintf("backup-%s-%d", path.Base(dir.Dir), backupSuffix)
		backupDir = path.Join(s.TargetDir, backupDir)
		log.Printf("[SnapshotFileWriter] original directory:(%s) rename to(%s)\n", targetDir, backupDir)
		if err = os.Rename(targetDir, backupDir); err != nil {
			return errors.Wrapf(err, "[SnapshotFileWriter] rename (%s) to (%s) failed\n", targetDir, backupDir)
		}

		tmpDir := path.Join(s.TmpDir, dir.Dir)
		log.Printf("[SnapshotFileWriter] tmp directory:(%s) rename to target:(%s)\n", tmpDir, targetDir)
		if err = os.Rename(tmpDir, targetDir); err != nil {
			return errors.Wrapf(err, "[SnapshotFileWriter] rename %s to %s failed", tmpDir, targetDir)
		}
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
	Meta     map[string]string `json:"meta"`

	ParentDir string `json:"-"` // This directory is different between leader and peers
}

func NewFile() *File {
	return &File{
		Meta: make(map[string]string),
	}
}

func (f *File) OpenReader() (CloseReader, error) {
	// FIXME: User has the responsibility to close the file handle
	fh, err := os.OpenFile(f.Fullname(), os.O_RDONLY, snapDirPerm)
	if err != nil {
		return nil, errors.Wrapf(err, "[File] open file(%s) failed. %v", f.Fullname(), err)
	}
	return fh, nil
}

func (f *File) OpenWriter() (CloseWriter, error) {
	// FIXME: User has the responsibility to close the file handle
	fh, err := os.OpenFile(f.Fullname(), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, snapFilePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "[File] open file(%s) failed. %v", f.Fullname(), err)
	}
	return fh, nil
}

func (f *File) SetParentDir(pd string) {
	f.ParentDir = pd
}

func (f *File) Marshal() (p []byte, err error) {
	p, err = json.Marshal(f)
	return
}

func (f *File) Unmarshal(p []byte) (err error) {
	err = json.Unmarshal(p, f)
	return
}

func (f *File) Metadata() map[string]string {
	return f.Meta
}

func (f *File) Fullname() string {
	log.Printf("[File] parent dir:(%s) filename:(%s)\n", f.ParentDir, f.Filename)
	return path.Join(f.ParentDir, f.Filename)
}
