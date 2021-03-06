// pongo2rethink provides a pongo2.TemplateLoader that pulls template's from RethinkDB.
//
//	opts := pongo2rethink.Opts{
//   TableName: "templates",
//   Prefix:    "randocustomer",
//   Session:   s.rethink,
//  }
//  dbtmpl := pongo2.NewSet("assetfs", pongo2rethink.NewPongoLoader(&opts))
//  res := dbtmpl.RenderTemplateFile("templates/tiny.pongo", pongo2.Context{"name": "florian"})
//
// It also has some additional methods to let you manage your templates.
// You can also use it with vanilla go templates via something like github.com/unrolled/render by
// using the RethinkLoader ala:
//
//  opts := pongo2rethink.Opts{
//   TableName: "templates",
//   Prefix:    "randocustomer",
//   Session:   s.rethink,
//  }
//  l := pongo2rethink.NewRethinkLoader(&opts))
//  render.New(render.Options{
//   Asset: l.GetTemplateBytes,
//   AssetNames: l.GetTemplateNames,
//  })
//
package pongo2rethink

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	rethink "github.com/dancannon/gorethink"
	"github.com/flosch/pongo2"
)

type Opts struct {
	TableName string           //Table to use
	Prefix    string           //optional prefix to apply to all paths
	Session   *rethink.Session //The rethink session that should be used
	Owner     string           //What if anything to put in an "owner" field, empty string is omitted
}

type RethinkTemplateLoader struct {
	r *Opts
}

func NewPongoLoader(opts *Opts) pongo2.TemplateLoader {
	return &RethinkTemplateLoader{r: opts}
}

func NewRethinkLoader(opts *Opts) *RethinkTemplateLoader {
	return &RethinkTemplateLoader{r: opts}
}

type Template struct {
	Name  string
	Data  string
	Owner string `gorethink:",omitempty"`
}

func (t *RethinkTemplateLoader) fetchTemplate(path string) (*Template, error) {
	var template Template
	rpath := t.r.Owner + t.r.Prefix + "/" + path
	log.Println("fetching:", rpath)
	cursor, err := rethink.Table(t.r.TableName).Get(rpath).Run(t.r.Session)
	if err != nil {
		return &template, err
	}
	err = cursor.One(&template)
	cursor.Close()
	if err == rethink.ErrEmptyResult {
		log.Println("Template not found:", rpath)
		return &template, fmt.Errorf("Template not found.")
	}
	return &template, err
}

// GetTemplate returns a Template object from rethink
func (t *RethinkTemplateLoader) GetTemplate(path string) (*Template, error) {
	return t.fetchTemplate(path)
}

// LoadTemplate inserts a given Template into rethink
func (t *RethinkTemplateLoader) LoadTemplate(template Template) error {
	result, err := rethink.Table(t.r.TableName).Insert(template).RunWrite(t.r.Session)
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
	template.Owner = t.r.Owner
	template.Name = t.r.Owner + t.r.Prefix + "/" + path
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	template.Data = string(data)
	result, err := rethink.Table(t.r.TableName).Insert(template).RunWrite(t.r.Session)
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
	template.Owner = t.r.Owner
	for _, tfile := range tfiles {
		template.Name = t.r.Owner + t.r.Prefix + "/" + tfile
		data, err := ioutil.ReadFile(tfile)
		if err != nil {
			return err
		}
		template.Data = string(data)
		// TODO: batch insert ?
		result, err := rethink.Table(t.r.TableName).Insert(template, rethink.InsertOpts{Conflict: "replace"}).RunWrite(t.r.Session)
		if err != nil {
			return err
		}
		if result.Inserted == 0 && result.Replaced == 0 && result.Unchanged == 0 {
			return fmt.Errorf("Encountered weird state on template insert: %s", fmt.Sprintf("%+v", result))
		}
	}
	return nil
}

// GetTemplateBytes retrieves a templates byte from rethink
func (t *RethinkTemplateLoader) GetTemplateBytes(path string) ([]byte, error) {
	log.Println("fetching:", path)
	template, err := t.fetchTemplate(path)
	return []byte(template.Data), err
}

// GetTemplateString retrieves a templates string from rethink
func (t *RethinkTemplateLoader) GetTemplateString(path string) (string, error) {
	template, err := t.fetchTemplate(path)
	return template.Data, err
}

// GetTemplateNames retrieves a list of all current template names from rethink
// It goes against standards in that it ONLY PRINTS ERRORS if it encounters any,
// to satisfy github.com/unrolled/render AssetNames()
func (t *RethinkTemplateLoader) GetTemplateNames() []string {
	var err error
	var cursor *rethink.Cursor
	if t.r.Owner == "" {
		cursor, err = rethink.Table(t.r.TableName).Pluck("Name").Run(t.r.Session)
	} else {
		cursor, err = rethink.Table(t.r.TableName).Filter(map[string]string{"Owner": "randocustomer"}).Pluck("Name").Run(t.r.Session)
	}
	if err != nil {
		log.Println("Error trying to list template names:", err)
	}
	defer cursor.Close()
	var resp map[string]string
	pfx := t.r.Owner + t.r.Prefix + "/"
	var names []string
	for cursor.Next(&resp) {
		names = append(names, strings.TrimPrefix(resp["Name"], pfx))
	}
	if cursor.Err() != nil {
		log.Println(cursor.Err)
	}
	return names
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
