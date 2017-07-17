package load

import (
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
)

type Service struct {
	mu     sync.Mutex
	config Config

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		config: c,
		logger: l,
	}
}

// TaskFiles gets a slice of all files with the .tick file extension
// and any associated files with .json, .yml, and .yaml file extentions
// in the configured task directory.
func (s *Service) TaskFiles() (tickscripts []string, tmplVars []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasksDir := s.config.TasksDir()

	files, err := ioutil.ReadDir(tasksDir)
	if err != nil {
		return nil, nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		switch ext := filepath.Ext(filename); ext {
		case ".tick":
			tickscripts = append(tickscripts, filepath.Join(tasksDir, filename))
		case ".yml", ".json", ".yaml":
			tmplVars = append(tmplVars, filepath.Join(tasksDir, filename))
		default:
			continue
		}
	}

	return
}

// HandlerFiles gets a slice of all files with the .json, .yml, and
// .yaml file extentions in the configured handler directory.
func (s *Service) HandlerFiles() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handlers := []string{}

	handlersDir := s.config.HandlersDir()

	files, err := ioutil.ReadDir(handlersDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		switch ext := filepath.Ext(filename); ext {
		case ".yml", ".json", ".yaml":
			handlers = append(handlers, filepath.Join(handlersDir, filename))
		default:
			continue
		}
	}

	return handlers, nil
}

func (s *Service) Load() error {
	return nil
}

func (s *Service) loadTickscripts() error {
	return nil
}

func (s *Service) loadTemplateVars() error {
	return nil
}

func (s *Service) loadHandlers() error {
	return nil
}
