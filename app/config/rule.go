package config

import "strings"

type Rule struct {
	Database     string            `toml:"database"`
	Table        string            `toml:"table"`
	Index        string            `toml:"index"`
	Type         string            `toml:"type"`
	Parent       string            `toml:"parent"`
	ID           string            `toml:"id"`
	FieldMapping map[string]string `toml:"field"`
}

func (r *Rule) InitRule() {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	// ES must use a lower-case Type
	// Here we also use for Index
	r.Index = strings.ToLower(r.Index)
	r.Type = strings.ToLower(r.Type)
}


