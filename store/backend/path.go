package backend

import (
	"path"
)

func genStoreDirPath(dir, prefix string) string {
	logdir := path.Join(dir, prefix)
	return logdir
}

func genSnapshotDirPath(dir, prefix string) string {
	return path.Join(dir, prefix)
}
