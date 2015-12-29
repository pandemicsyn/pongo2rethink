// pongo2rethink provides a pongo2.TemplateLoader that pulls template's from RethinkDB.
//
//  opts := pongo2rethink.Opts{
//		DatabaseName: "test",
//      TableName: "templates",
//      Prefix: "somePrefix",
//      Session: someRethinkSession,
//  }}
//  dbtmpl := pongo2.NewSet("assetfs", pongo2rethink.New(opts))
//
// It also has some additional methods to let you manage your templates.
package pongo2rethink

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"

	rethink "github.com/dancannon/gorethink"
	"github.com/flosch/pongo2"
)

type Opts struct {
	DatabaseName string           //Database to use
	TableName    string           //Table to use
	Prefix       string           //optional prefix to apply to all paths
	Session      *rethink.Session //The rethink session that should be used
}

type RethinkTemplateLoader struct {
	r *Opts
}

func New(opts *Opts) pongo2.TemplateLoader {
	return &RethinkTemplateLoader{r: opts}
}

type Template struct {
	Name string
	Data string
}

func (t *RethinkTemplateLoader) fetchTemplate(path string) (*Template, error) {
	var template Template
	rpath := t.r.Prefix + "/" + path
	cursor, err := rethink.Table(t.r.DatabaseName).Get(rpath).Run(t.r.Session)
	if err != nil {
		return &template, err
	}
	err = cursor.One(&template)
	cursor.Close()
	return &template, err
}

// GetTemplate returns a Template object from rethink
func (t *RethinkTemplateLoader) GetTemplate(path string) (*Template, error) {
	return t.fetchTemplate(path)
}

// LoadTemplate inserts a given Template into rethink
func (t *RethinkTemplateLoader) LoadTemplate(template Template) error {
	result, err := rethink.Table(t.r.DatabaseName).Insert(template).RunWrite(t.r.Session)
	if err != nil {
		return err
	}
	if result.Inserted == 0 && result.Replaced == 0 {
		return fmt.Errorf("Encountered weird state on template insert: %s", fmt.Sprintf("%+v", result))
	}
	return err
}

// LoadTemplateFromFile reads a given file and loads it into rethink
func (t *RethinkTemplateLoader) LoadTemplateFromFile(path string) (err error) {
	var template Template
	template.Name = path
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	template.Data = string(data)
	result, err := rethink.Table(t.r.DatabaseName).Insert(template).RunWrite(t.r.Session)
	if err != nil {
		return err
	}
	if result.Inserted == 0 && result.Replaced == 0 {
		return fmt.Errorf("Encountered weird state on template insert: %s", fmt.Sprintf("%+v", result))
	}
	return nil
}

// LoadTemplatesFromDir walks a given path recursively searching for the provided shell pattern
// and loads any encountered templates into rethink
func (t *RethinkTemplateLoader) LoadTemplatesFromDir(dir, pattern string) error {
	tfiles, err := FindTemplates(dir, pattern)
	if err != nil {
		return err
	}
	var template Template
	for _, tfile := range tfiles {
		template.Name = tfile
		data, err := ioutil.ReadFile(tfile)
		if err != nil {
			return err
		}
		template.Data = string(data)
		// TODO: batch insert ?
		result, err := rethink.Table(t.r.DatabaseName).Insert(template).RunWrite(t.r.Session)
		if err != nil {
			return err
		}
		if result.Inserted == 0 && result.Replaced == 0 {
			return fmt.Errorf("Encountered weird state on template insert: %s", fmt.Sprintf("%+v", result))
		}
	}
	return nil
}

// GetTemplateBytes retrieves a templates byte from rethink
func (t *RethinkTemplateLoader) GetTemplateBytes(path string) ([]byte, error) {
	template, err := t.fetchTemplate(path)
	return []byte(template.Data), err
}

// GetTemplateString retrieves a templates string from rethink
func (t *RethinkTemplateLoader) GetTemplateString(path string) (string, error) {
	template, err := t.fetchTemplate(path)
	return template.Data, err
}

// Abs returns the absolute path and is used by pongo2
func (t *RethinkTemplateLoader) Abs(base, name string) string {
	if filepath.IsAbs(name) || base == "" {
		return name
	}
	if name == "" {
		return base
	}
	return filepath.Dir(base) + string(filepath.Separator) + name
}

// Get returns io.Reader of a template contents from rethink and is used by pongo2
func (t *RethinkTemplateLoader) Get(path string) (io.Reader, error) {
	data, err := t.GetTemplateBytes(path)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}
