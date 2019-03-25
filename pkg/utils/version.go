package utils

import (
	"fmt"

	"github.com/amyangfei/data-dam/pkg/log"
)

// Version information.
var (
	ReleaseVersion = "None"
	BuildTS        = "None"
	GitHash        = "None"
	GitBranch      = "None"
	GoVersion      = "None"
)

// GetRawInfo gets app's release related info
func GetRawInfo(binName string) string {
	var info string
	info += fmt.Sprintf("%s\n", binName)
	info += fmt.Sprintf("Release Version: %s\n", ReleaseVersion)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", GoVersion)
	return info
}

// PrintInfo prints information fetched from `GetRawInfo`
func PrintInfo(app string, callback func()) {
	oriLevel := log.GetLogLevelAsString()
	log.SetLevelByString("info")
	printInfo(app)
	callback()
	log.SetLevelByString(oriLevel)
}

func printInfo(app string) {
	log.Infof("Welcome to %s", app)
	log.Infof("Release Version: %s", ReleaseVersion)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Git Branch: %s", GitBranch)
	log.Infof("UTC Build Time: %s", BuildTS)
	log.Infof("Go Version: %s", GoVersion)
}
