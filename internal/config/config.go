package config

import (
	mymap "JacuteSQL/internal/data_structures/mymap"
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Schema struct {
	Name        string           `json:"name"`
	TuplesLimit int              `json:"tuples_limit"`
	Tables      *mymap.CustomMap `json:"structure"`
}

type Config struct {
	Env          string        `yaml:"env" env-default:"prod"`
	StoragePath  string        `yaml:"storage_path" env-required:"true"`
	SchemaPath   string        `yaml:"schema_path" env-required:"true"`
	LogPath      string        `yaml:"log_path" env-required:"true"`
	Port         int           `yaml:"port" env-default:"7432"`
	ConnTL       time.Duration `yaml:"connTL" env-default:"0s"`
	LoadedSchema *Schema
}

func MustLoad() *Config {
	path := fetchConfigPath()
	if path == "" {
		panic("Config path not provided")
	}

	config := MustLoadByPath(path)

	return config
}

func MustLoadByPath(path string) *Config {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		panic("Config file not found: " + path)
	}

	var config Config
	if err := cleanenv.ReadConfig(path, &config); err != nil {
		panic("Couldn't read config: " + err.Error())
	}
	config.LoadedSchema = Parse(config.SchemaPath)
	return &config
}

func fetchConfigPath() string {
	var path string

	flag.StringVar(&path, "config", "", "Path to config file")
	flag.Parse()

	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}

	return path
}

func Parse(schemaPath string) *Schema {
	if _, err := os.Stat(schemaPath); err != nil {
		panic("Invalid schema path " + schemaPath + ": " + err.Error())
	}

	file, err := os.ReadFile(schemaPath)
	if err != nil {
		panic("Can't read schema file " + schemaPath + ": " + err.Error())
	}
	schema := Schema{}
	if err := json.Unmarshal(file, &schema); err != nil {
		panic("Can't parse json schema file " + err.Error())
	}
	return &schema
}
