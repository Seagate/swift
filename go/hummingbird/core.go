package hummingbird

type ObjectWriter interface {
	Write(data []byte) (n int, err error)
	SetMetadata(md map[string]string) error
	Finalize() error	
}

type Store interface {
	// Not happy about needing to abstract this method...
	ObjNames(string) (string, string)	
	CreateWriter(string, string) (ObjectWriter, error)
}