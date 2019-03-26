package utils

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReleaseInfo(t *testing.T) {
	ReleaseVersion = "test-dev"
	BuildTS = time.Now().In(time.UTC).Format("2016-01-02 15:04:05")
	GitHash = "hash"
	GitBranch = "branch"
	GoVersion = runtime.Version()
	binName := "go-logster"
	expected := strings.Join(
		[]string{
			binName,
			"Release Version: " + ReleaseVersion,
			"Git Commit Hash: " + GitHash,
			"Git Branch: " + GitBranch,
			"UTC Build Time: " + BuildTS,
			"Go Version: " + GoVersion,
		}, "\n") + "\n"
	info := GetRawInfo(binName)
	assert.Equal(t, expected, info)
}
