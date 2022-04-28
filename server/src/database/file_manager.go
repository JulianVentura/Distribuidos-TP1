package database

import (
	"fmt"
	"io/ioutil"
	"sync"

	log "github.com/sirupsen/logrus"
)

type FileManager struct {
	general_lock sync.RWMutex
	file_locks   map[string]*sync.RWMutex
}

func NewFileManager(files_path string) (*FileManager, error) {

	self := &FileManager{
		general_lock: sync.RWMutex{},
		file_locks:   make(map[string]*sync.RWMutex),
	}

	err := self.initialize_lock_structure(files_path)
	if err != nil {
		return nil, err
	}

	return self, nil
}

func (self *FileManager) get_lock_for(file string) (*sync.RWMutex, error) {
	self.general_lock.RLock()
	defer self.general_lock.RUnlock()

	lock, exists := self.file_locks[file]
	if !exists {
		return nil, fmt.Errorf("File %v do not exist", file)
	}

	return lock, nil
}

func (self *FileManager) add_new_files(files []string) error {
	self.general_lock.Lock()
	defer self.general_lock.Unlock()

	for _, file := range files {
		_, exists := self.file_locks[file]
		if exists {
			return fmt.Errorf("File %v already exists", file)
		}
	}

	for _, file := range files {
		log.Debugf("Created file lock for %v\n", file)
		self.file_locks[file] = &sync.RWMutex{}
	}

	return nil
}

func (self *FileManager) initialize_lock_structure(path string) error {

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return fmt.Errorf("Couldn't read %v directory", path)
	}

	for _, file := range files {
		log.Debugf("Created file lock for %v\n", file.Name())
		self.file_locks[file.Name()] = &sync.RWMutex{}
	}

	return nil
}
