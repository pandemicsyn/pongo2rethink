package pongo2rethink

import (
	"os"
	"path/filepath"
)

type templateFinder struct {
	path    string
	pattern string
	matches []string
}

func (f *templateFinder) walker(path string, info os.FileInfo, err error) error {
	if !info.IsDir() {
		matched, _ := filepath.Match(f.pattern, info.Name())
		if matched {
			f.matches = append(f.matches, path)
		}
	}
	return err
}

// FindTemplates walks a given path and returns a slice of files that match
// the provided shell pattern.
func FindTemplates(path, pattern string) ([]string, error) {
	f := &templateFinder{
		path:    path,
		pattern: pattern,
		matches: make([]string, 0),
	}
	err := filepath.Walk(f.path, f.walker)
	return f.matches, err
}
