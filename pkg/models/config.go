package models

// DBConfig is the full database set configuration
type DBConfig struct {
	Verbose bool        `toml:"verbose" json:"verbose"` // verbose logging
	MySQL   MySQLConfig `toml:"mysql" json:"mysql"`     // mysql config
}

// MySQLConfig stores mysql config
type MySQLConfig struct {
	Host     string `toml:"host" json:"host"`
	Port     int    `toml:"port" json:"port"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Enabled  bool   `toml:"enabled" json:"enabled"`
}
