package backend

import "fmt"

type BackendType int

const (
	BackendTypeLevelDB BackendType = iota
)

func (b BackendType) String() string {
	switch b {
	case BackendTypeLevelDB:
		return "backend-leveldb"
	default:
		return "unknow"
	}
}

func Exist(typ BackendType, dir string) bool {
	switch typ {
	case BackendTypeLevelDB:
		return leveldbExist(dir)
	default:
		panic(fmt.Sprintf("unknow type %v in %s", typ, dir))
	}
}
