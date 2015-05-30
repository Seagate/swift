package stores

import (
	"fmt"
	"path/filepath"
    "log/syslog"
	"io/ioutil"
	"os"
	"strconv"
    	
	"github.com/openstack/swift/go/hummingbird"	
    "github.com/seagate/kinetic-go/kinetic"
)

// Store
type KineticStore struct { 
	logger *syslog.Writer
}

func NewKineticStore(logger *syslog.Writer) KineticStore {
	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
	return KineticStore { logger: logger }
}

func (self *KineticStore) ObjNames(base string) (string, string) {
	return filepath.Join(base, "foo.data"), filepath.Join(base, "foo.meta")	
}

func (self *KineticStore) CreateWriter(device string, name ObjectName, length int, timestamp string) (KineticWriter, error) {
	Info.Println(fmt.Sprintf("Creating KineticWriter for %s @ %s", name.Object, device))
    client, _ := kinetic.Connect(device)
	
	limit := 1024*1024
	
	return KineticWriter { name: name, 
						   timestamp: timestamp,
                           client:   client, 
                           logger:   self.logger,
						   length:   length,
                           metadata: make(map[string]string),						   				
						   limit: limit,
						   count: 0,
						   transfered: 0,	
						   left : length,					   
						   ch: nil, }, nil
}

func buildChunkKey(name ObjectName, timestamp string, index int) string {
	return fmt.Sprintf("%s/objects/%s.%s.%d.chunk", 
		name.Partition, 
		name.Hash(),
		timestamp,
		index)
} 

func buildMetadataKey(name ObjectName) string {
	return fmt.Sprintf("%s/metadata/%s.md", 
		name.Partition, 
		name.Hash())
} 

// Writer

type KineticWriter struct {	
	name	   ObjectName
	timestamp  string
	logger     *syslog.Writer
    client     kinetic.Client
	length     int
    metadata   map[string]string  
	limit      int
	count      int
	transfered int
	left       int
	ch         chan []byte	
}


func (self *KineticWriter) logit(key string, length int){
	Info.Println(fmt.Sprintf("> Promising %d bytes", length))
	rx, err := self.client.PutFrom([]byte(key), length, self.ch)
	if err != nil {
		Error.Println(err)
	}
	err = <-rx
	if err != nil {
		Error.Println(err)
	}else {
		Info.Println(fmt.Sprintf("> Got reply for subkey %s", key))
	}
}

func (self *KineticWriter) startPut() error {
	Info.Println(fmt.Sprintf("Starting Put"))
	
	length := self.left
	if self.left > self.limit { 
		length = self.limit 
	}	
	
	self.ch = make(chan []byte)
	key := buildChunkKey(self.name, self.timestamp, self.count)
	// todo, add receiver to some pending list
	go self.logit(key, length)
	self.count += 1
	return nil
}

func (self *KineticWriter) Write(data []byte) (int, error) {
    ln := len(data)
	left := ln
	
	Info.Println(fmt.Sprintf("Writing %d bytes for %s", ln, self.name))
	
	for left > 0 {
		
		if self.transfered == 0 {
			self.startPut()
		}
		
		// If we are gonna go over
		if left + self.transfered >= self.limit {
			// send only what fits 
			rest := self.limit - self.transfered
			offset := ln - left
			self.ch <- data[offset:offset+rest]
			self.ch <- nil // signal end
			Info.Println(fmt.Sprintf("    Kinetic object %d sent...", self.count-1))
			self.transfered = 0			
			left -= rest
		} else {
			// otherwise, send whatever is left
			Info.Println(fmt.Sprintf("    Transfering %d bytes that were left...", left))
			self.ch <- data[ln-left:]
			self.transfered += left
			break
		}
	} 
	
	self.left -= ln
	Info.Println(fmt.Sprintf("    Thare are %d bytes left...", self.left))
		
	if self.left == 0 && self.transfered > 0 {
		Info.Println(fmt.Sprintf("    Terminating pending channel."))
		self.ch <- nil // we are done
	}
	
	return ln, nil // always tell them we wrote it
}

func (self *KineticWriter) SetMetadata(md map[string]string) error {
    Trace.Println(fmt.Sprintf("Received metadata for %s", self.name))
	self.metadata = md
    return nil
}

func (self *KineticWriter) Finalize() error {    
    Trace.Println(fmt.Sprintf("Finalizing %s, wrote %d pieces", self.name, self.count))	
	defer self.client.Close()
	
	// TODO: invalidate the writer so it can't be used again
	self.metadata["kinetic-object-count"] = strconv.Itoa(self.count)
	bytes := hummingbird.PickleDumps(self.metadata)
	key := buildMetadataKey(self.name)
    rx, err := self.client.Put([]byte(key), bytes)
	if err != nil { return err }
	
	Info.Println("Finalized Kinetic PUT")
	
	// TODO: should probably wait for all pending

	return <-rx
}

