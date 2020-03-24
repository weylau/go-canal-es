package app

import (
	"os"
	"os/exec"
	"path/filepath"
)

func GetAppDir() string {
	appDir, err := os.Getwd()
	if err != nil {
		file, _ := exec.LookPath(os.Args[0])
		applicationPath, _ := filepath.Abs(file)
		appDir, _ = filepath.Split(applicationPath)
	}
	return appDir
}