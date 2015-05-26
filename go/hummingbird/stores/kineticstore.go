package stores

import (
	"fmt"
	"path/filepath"
    "log/syslog"
	"io/ioutil"
	"os"
    
    "github.com/openstack/swift/go/hummingbird"
    "github.com/seagate/kinetic-go/kinetic"
)

// Store
type KineticStore struct { 
	logger *syslog.Writer
}

func NewKineticStore(logger *syslog.Writer) KineticStore {
	InitLog(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
	return KineticStore { logger: logger }
}

func (self *KineticStore) ObjNames(base string) (string, string) {
	return filepath.Join(base, "foo.data"), filepath.Join(base, "foo.meta")	
}

func (self *KineticStore) CreateWriter(target string, name string) (KineticWriter, error) {
	Info.Println(fmt.Sprintf("Creating KineticWriter for %s @ %s", name, target))
    client, _ := kinetic.Connect(target)
	
	limit := 1024*1024
	
	return KineticWriter { name: name, 
                           client: client, 
                           logger: self.logger,
                           metadata: make(map[string]string),
						   chunk: make([]byte, limit),
						   buffered: 0,
						   limit: limit,
						   count: 0 }, nil
}

// Writer

type KineticWriter struct {	
	name     string	
	logger   *syslog.Writer
    client   kinetic.Client
    metadata map[string]string  
	chunk    []byte
	buffered int
	limit    int
	count    int
}

func (self *KineticWriter) Write(data []byte) (int, error) {
    ln := len(data)
	Trace.Println(fmt.Sprintf("Writing %i bytes for %s", ln, self.name))
	if ln + self.buffered >= self.limit {
		space := self.limit - self.buffered
		copy(self.chunk[self.buffered:], data[:space])
		// with the chunk full, we can flush it
		key := self.name + "." + string(self.count)
		self.client.Put([]byte(key), self.chunk)
		Trace.Println(fmt.Sprintf("    Kinetic object %i sent...", self.count))
		self.chunk = make([]byte, self.limit) // so inneficient....
		self.buffered = ln - space
		if self.buffered > 0 {
			copy(self.chunk, data[space:]) // ugh...
		}
		self.count += 1
	} else {
		copy(self.chunk[self.buffered:], data) // inneficient, so inneficient...
		self.buffered += ln
	}
	Trace.Println(fmt.Sprintf("  We have %i bytes so far...", self.buffered))
	
	return ln, nil // always tell them we wrote it
}

func (self *KineticWriter) SetMetadata(md map[string]string) error {
    Trace.Println(fmt.Sprintf("Received metadata for %s", self.name))
	self.metadata = md
    return nil
}

func (self *KineticWriter) Finalize() error {    
    Trace.Println(fmt.Sprintf("Finalizing %s", self.name))
	if self.buffered > 0 {
		Info.Println(fmt.Sprintf("    Writing tail of %i bytes", self.buffered))
		key := self.name + "." + string(self.count)
		self.client.Put([]byte(key), self.chunk[:self.buffered])
		self.count += 1
	}
	Trace.Println(fmt.Sprintf("Wrote %i pieces", self.count))
	
	// TODO: invalidate the writer so it can't be changed
	self.metadata["kinetic-object-count"] = string(self.count)
	bytes := hummingbird.PickleDumps(self.metadata)
    rx, _ := self.client.Put([]byte(self.name + ".md"), bytes)
	Info.Println("Finalized Kinetic PUT")
	return <-rx
}

