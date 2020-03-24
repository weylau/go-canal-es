package app

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"io/ioutil"
	"time"
)

type Config struct {
	ESHttps    bool   `toml:"es_https"`
	ESAddr     string `toml:"es_addr"`
	ESUser     string `toml:"es_user"`
	ESPassword string `toml:"es_pass"`
	LogLevel   string `toml:"log_level"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime TomlDuration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`
	Debug bool `toml:"debug"`
}

type TomlDuration struct {
	time.Duration
}

var Conf *Config

func InitConfig(configfile string) (error) {
	data, err := ioutil.ReadFile(configfile)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = toml.Decode(string(data), &Conf)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}