package central

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/pkg/log"
	"github.com/amyangfei/data-dam/pkg/models"
	"github.com/amyangfei/data-dam/pkg/utils"
)

const (
	defaultAppName string = "central controller"
)

// Config is the configuration
type Config struct {
	flagSet *flag.FlagSet

	LogLevel string `toml:"log-level" json:"log-level"`
	LogFile  string `toml:"log-file" json:"log-file"`

	ConfigFile string `json:"config-file"`

	Seconds    int64           `json:"-"`
	Rate       int             `toml:"rate" json:"rate"`
	Duration   string          `toml:"duration" json:"duration"`
	Concurrent int             `toml:"concurrent" json:"concurrent"`
	DBConfig   models.DBConfig `toml:"db-config" json:"db-config"`
	OpWeight   []int           `toml:"op-weight" json:"op-weight"`
	Schemas    []string        `toml:"schemas" json:"schemas"`

	printVersion bool
}

// NewConfig creates a new base config for central.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("central", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "log/data-dam-central.log", "log file path")
	fs.IntVar(&cfg.Rate, "rate", 5, "number of requests per time unit (5/1s)")
	fs.StringVar(&cfg.Duration, "duration", "10s", "test duration (0 = forever)")
	fs.IntVar(&cfg.Concurrent, "concurrent", 10, "concurrent for database")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo(defaultAppName))
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return errors.Trace(c.veirfy())
}

func (c *Config) veirfy() error {
	d, err := time.ParseDuration(c.Duration)
	if err != nil {
		return errors.Trace(err)
	}
	c.Seconds = int64(d.Seconds())

	// TODO: currently support MySQL only, add more database support later
	if !c.DBConfig.MySQL.Enabled {
		return errors.New("support MySQL/MariaDB only")
	}

	if len(c.OpWeight) != len(models.RealOpType) {
		c.OpWeight = models.DefaultOpWeiht
	}

	return nil
}

// String returns format string of Config
func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Errorf("marshal config to json error %v", err)
	}
	return string(cfg)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
