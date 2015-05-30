package stores

import (
	"io"
	"crypto/md5"
	"encoding/hex"
)

type ObjectWriter interface {
	Write(data []byte) (n int, err error)
	SetMetadata(md map[string]string) error
	Finalize() error	
}

type Store interface {
	CreateWriter(device string, name ObjectName, length int, timestamp string) (ObjectWriter, error)
}

type ObjectName struct {
	prefix    string
	Partition string	
	Account   string 
	Container string
	Object    string
	suffix    string
}

func NewObjectName(vars map[string]string, prefix string, suffix string) ObjectName {
	return ObjectName { prefix: prefix,
						Partition: vars["partition"],
	                    Account: vars["account"],
						Container: vars["caontainer"], 
						Object: vars["object"],
						suffix: suffix, }
}

func (self ObjectName) Hash() string {
	h := md5.New()
	io.WriteString(h, self.prefix+"/"+self.Account+"/"+self.Container+"/"+self.Object+self.suffix)
	hexHash := hex.EncodeToString(h.Sum(nil))
	return hexHash	
}