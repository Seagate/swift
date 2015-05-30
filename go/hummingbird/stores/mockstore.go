package stores

import (
	"fmt"
    "log/syslog"
	"io/ioutil"
	"os"
)

// Store
type MockStore struct { 
	logger *syslog.Writer
}

func NewMockStore(logger *syslog.Writer) MockStore {
	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
	return MockStore { logger: logger }
}

func (self *MockStore) CreateWriter(device string,  name ObjectName, length int, timestamp string) (MockWriter, error) {
	Info.Println(fmt.Sprintf("Creating MockWriter for %s", name.Object))
	return MockWriter { name: name, logger: self.logger }, nil
}

// Writer

type MockWriter struct {
	name   ObjectName	
	logger *syslog.Writer
}

func (self *MockWriter) Write(data []byte) (int, error) {
	Info.Println(fmt.Sprintf("Writing %i bytes for %s", len(data), self.name.Object))
	return len(data), nil
}

func (self *MockWriter) SetMetadata(md map[string]string) error {
	Info.Println(fmt.Sprintf("Received metadata for %s", self.name.Object))
	return nil
}

func (self *MockWriter) Finalize() error {
	Info.Println(fmt.Sprintf("Finalizing %s", self.name.Object))
	return nil
}

