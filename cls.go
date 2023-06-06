package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type File struct {
	Name    string
	IsOpen  bool
	Content string
	Mutex   sync.Mutex
}

type DistributedFileSystem struct {
	Files            map[string]*File
	FilesMutex       sync.Mutex
	Requests         []*Request
	RequestMutex     sync.Mutex
	Acknowledged     []bool
	AcknowledgeMutex sync.Mutex
	Timestamps       []int
	TimestampMutex   sync.Mutex
	LogFile          *os.File
	DeferredArray    []string
}

type Client struct {
	ID       int
	FileName string
}

type Request struct {
	ClientID  int
	File      *File
	Timestamp int
}

func (fs *DistributedFileSystem) OpenFile(clientID int, fileName string) *File {
	fs.FilesMutex.Lock()
	defer fs.FilesMutex.Unlock()

	file, ok := fs.Files[fileName]
	if !ok {
		fileContent, err := ioutil.ReadFile(fileName)
		if err != nil {
			fmt.Printf("Error opening file %s: %v\n", fileName, err)
			return nil
		}

		file = &File{
			Name:    fileName,
			IsOpen:  true,
			Content: string(fileContent),
		}
		fs.Files[fileName] = file
	} else {
		file.Mutex.Lock()
		file.IsOpen = true
		file.Mutex.Unlock()
	}

	fmt.Printf("Client %d opened file %s\n", clientID, fileName)
	return file
}

func (fs *DistributedFileSystem) CloseFile(file *File) {
	file.Mutex.Lock()
	file.IsOpen = false
	file.Mutex.Unlock()
	fmt.Printf("File %s closed\n", file.Name)
}

func (fs *DistributedFileSystem) ReadFile(clientID int, file *File) {
	fs.RequestMutex.Lock()
	defer fs.RequestMutex.Unlock()

	fs.TimestampMutex.Lock()
	timestamp := len(fs.Timestamps) + 1
	fs.Timestamps = append(fs.Timestamps, timestamp)
	fs.TimestampMutex.Unlock()

	request := &Request{
		ClientID:  clientID,
		File:      file,
		Timestamp: timestamp,
	}

	fs.Requests = append(fs.Requests, request)

	for i := range fs.Requests {
		if fs.Requests[i].ClientID != clientID {
			go fs.SendRequest(clientID, fs.Requests[i])
		}
	}

	for i := range fs.Requests {
		if fs.Requests[i].ClientID != clientID {
			fs.ReceiveAcknowledge()
		}
	}

	fmt.Printf("Client %d read file %s: %s\n", clientID, file.Name, file.Content)
	fs.LogRequest(clientID, "Read", file.Name, timestamp)
	fs.AddDeferredOperation(fmt.Sprintf("Read by Client %d", clientID))
}

func (fs *DistributedFileSystem) WriteFile(clientID int, file *File, content string) {
	fs.RequestMutex.Lock()
	defer fs.RequestMutex.Unlock()

	fs.TimestampMutex.Lock()
	timestamp := len(fs.Timestamps) + 1
	fs.Timestamps = append(fs.Timestamps, timestamp)
	fs.TimestampMutex.Unlock()

	request := &Request{
		ClientID:  clientID,
		File:      file,
		Timestamp: timestamp,
	}

	fs.Requests = append(fs.Requests, request)

	for i := range fs.Requests {
		if fs.Requests[i].ClientID != clientID {
			go fs.SendRequest(clientID, fs.Requests[i])
		}
	}

	for i := range fs.Requests {
		if fs.Requests[i].ClientID != clientID {
			fs.ReceiveAcknowledge()
		}
	}

	file.Mutex.Lock()
	file.Content = content
	file.Mutex.Unlock()

	err := ioutil.WriteFile(file.Name, []byte(content), 0644)
	if err != nil {
		fmt.Printf("Error writing to file %s: %v\n", file.Name, err)
		return
	}

	fmt.Printf("Client %d wrote to file %s: %s\n", clientID, file.Name, content)
	fs.LogRequest(clientID, "Write", file.Name, timestamp)
	fs.AddDeferredOperation(fmt.Sprintf("Write by Client %d", clientID))
}

func (fs *DistributedFileSystem) SendRequest(clientID int, request *Request) {
	fmt.Printf("Client %d sent request to client %d\n", clientID, request.ClientID)
	fs.AcknowledgeMutex.Lock()
	defer fs.AcknowledgeMutex.Unlock()

	if request.ClientID < len(fs.Acknowledged) {
		fs.Acknowledged[request.ClientID] = true
	} else {
		fmt.Println("Invalid client ID in SendRequest......")
	}
}

func (fs *DistributedFileSystem) ReceiveAcknowledge() {
	fs.AcknowledgeMutex.Lock()
	defer fs.AcknowledgeMutex.Unlock()

	for i := 0; i < len(fs.Acknowledged); i++ {
		if !fs.Acknowledged[i] {
			return
		}
	}

	fs.Acknowledged = make([]bool, len(fs.Requests))

	fmt.Println("All acknowledgments received")
}

func (fs *DistributedFileSystem) LogRequest(clientID int, action string, fileName string, timestamp int) {
	logEntry := fmt.Sprintf("Client %d %s file %s at timestamp %d\n", clientID, action, fileName, timestamp)
	fs.LogFile.WriteString(logEntry)
}

func (fs *DistributedFileSystem) AddDeferredOperation(operation string) {
	fs.DeferredArray = append(fs.DeferredArray, operation)
}

func printSpaceTimeDiagram(clientID int, startTime time.Time, endTime time.Time, outputFile *os.File) {
	fmt.Fprintf(outputFile, "Client %d: %s - %s\n", clientID, startTime.Format("15:04:05"), endTime.Format("15:04:05"))

	duration := endTime.Sub(startTime)

	spacesBefore := startTime.Hour()*3600 + startTime.Minute()*60 + startTime.Second()
	spacesAfter := (24 * 3600) - spacesBefore - int(duration.Seconds())

	spaceLine := ""
	for i := 0; i < spacesBefore; i++ {
		spaceLine += " "
	}
	for i := 0; i < int(duration.Seconds()); i++ {
		spaceLine += "-"
	}
	for i := 0; i < spacesAfter; i++ {
		spaceLine += " "
	}

	fmt.Fprintln(outputFile, spaceLine)
	fmt.Fprintln(outputFile)
}

func main() {
	fileSystem := &DistributedFileSystem{
		Files:            make(map[string]*File),
		FilesMutex:       sync.Mutex{},
		Requests:         []*Request{},
		RequestMutex:     sync.Mutex{},
		AcknowledgeMutex: sync.Mutex{},
		Timestamps:       []int{},
		TimestampMutex:   sync.Mutex{},
		DeferredArray:    []string{},
	}

	/* Make a Note of this ------ creating a log file so that we can keep a track of the previous state of the file which will be useful for the loopp crearion of the clients*/
	logFile, err := os.OpenFile("file_access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}
	defer logFile.Close()

	fileSystem.LogFile = logFile
	var numClients int
	fmt.Print("Enter the number of clients: ")
	fmt.Scanln(&numClients)
	fileSystem.Acknowledged = make([]bool, numClients)

	var wg sync.WaitGroup

	outputFile, err := os.Create("spacetime_diagram.txt")
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := &Client{
				ID:       clientID + 1,
				FileName: "file1.txt",
			}
			file := fileSystem.OpenFile(client.ID, client.FileName)
			if file != nil {
				startTime := time.Now()
				fileSystem.WriteFile(client.ID, file, fmt.Sprintf("Content written by Client %d", client.ID))
				fileSystem.ReadFile(client.ID, file)
				fileSystem.CloseFile(file)
				endTime := time.Now()
				printSpaceTimeDiagram(client.ID, startTime, endTime, outputFile)
			}
		}(i)
	}

	wg.Wait()

	fmt.Println("Deferred Array Operations:")
	for i, operation := range fileSystem.DeferredArray {
		fmt.Printf("%d. %s\n", i+1, operation)
	}
}
