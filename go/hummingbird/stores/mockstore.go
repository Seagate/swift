package stores

import (
	"fmt"
	"path/filepath"
    "log/syslog"
	"io/ioutil"
	"os"
)

// Store
type MockStore struct { 
	logger *syslog.Writer
}

func NewMockStore(logger *syslog.Writer) MockStore {
	InitLog(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
	return MockStore { logger: logger }
}

func (self *MockStore) ObjNames(base string) (string, string) {
	return filepath.Join(base, "foo.data"), filepath.Join(base, "foo.meta")	
}

func (self *MockStore) CreateWriter(target string, name string) (MockWriter, error) {
	Info.Println(fmt.Sprintf("Creating MockWriter for %s", name))
	return MockWriter { name: name, logger: self.logger }, nil
}

// Writer

type MockWriter struct {
	name   string	
	logger *syslog.Writer
}

func (self *MockWriter) Write(data []byte) (int, error) {
	Info.Println(fmt.Sprintf("Writing %i bytes for %s", len(data), self.name))
	return len(data), nil
}

func (self *MockWriter) SetMetadata(md map[string]string) error {
	Info.Println(fmt.Sprintf("Received metadata for %s", self.name))
	return nil
}

func (self *MockWriter) Finalize() error {
	Info.Println(fmt.Sprintf("Finalizing %s", self.name))
	return nil
}

