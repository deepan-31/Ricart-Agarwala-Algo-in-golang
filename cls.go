package main

import (
	"fmt"
	"io/ioutil"
	"sync"
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
}

type Client struct {
	ID       int
	FileName string
}

type Request struct {
	ClientID int
	File     *File
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

	request := &Request{
		ClientID: clientID,
		File:     file,
	}

	fs.Requests = append(fs.Requests, request)

	// we are broadcasting read req to all other clients
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
}

func (fs *DistributedFileSystem) WriteFile(clientID int, file *File, content string) {
	fs.RequestMutex.Lock()
	defer fs.RequestMutex.Unlock()

	request := &Request{
		ClientID: clientID,
		File:     file,
	}

	fs.Requests = append(fs.Requests, request)

	//we ate broadcasting write req to all other clients
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
}

func (fs *DistributedFileSystem) SendRequest(clientID int, request *Request) {
	fmt.Printf("Client %d sent request to client %d\n", clientID, request.ClientID)
	fs.AcknowledgeMutex.Lock()
	fs.Acknowledged[request.ClientID] = true
	fs.AcknowledgeMutex.Unlock()
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

func main() {
	fileSystem := &DistributedFileSystem{
		Files:            make(map[string]*File),
		FilesMutex:       sync.Mutex{},
		Requests:         []*Request{},
		RequestMutex:     sync.Mutex{},
		Acknowledged:     []bool{},
		AcknowledgeMutex: sync.Mutex{},
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		client1 := &Client{
			ID:       1,
			FileName: "file1.txt",
		}
		file := fileSystem.OpenFile(client1.ID, client1.FileName)
		if file != nil {
			fileSystem.WriteFile(client1.ID, file, "Deepan is bad boy")
			fileSystem.ReadFile(client1.ID, file)
			fileSystem.CloseFile(file)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client2 := &Client{
			ID:       2,
			FileName: "file1.txt",
		}
		file := fileSystem.OpenFile(client2.ID, client2.FileName)
		if file != nil {
			fileSystem.WriteFile(client2.ID, file, "Deepan is good boy")
			fileSystem.ReadFile(client2.ID, file)
			fileSystem.CloseFile(file)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client3 := &Client{
			ID:       3,
			FileName: "file1.txt",
		}
		file := fileSystem.OpenFile(client3.ID, client3.FileName)
		if file != nil {
			fileSystem.WriteFile(client3.ID, file, "Deepan is good boy")
			fileSystem.ReadFile(client3.ID, file)
			fileSystem.CloseFile(file)
		}
	}()

	wg.Wait()
}
