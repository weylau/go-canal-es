package app

// Package log 基础日志组件
import (
	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)


var Loger *logrus.Logger

func LogerDefault() *logrus.Logger {
	if Loger != nil {
		return Loger
	}
	setLevel()
	Loger = logrus.New()
	if Conf.Debug != true{
		logFileWriter := &logFileWriter{}
		Loger.SetOutput(logFileWriter)
	}
	return Loger
}


var levels = map[string]logrus.Level{
	"panic": logrus.PanicLevel,
	"fatal": logrus.FatalLevel,
	"error": logrus.ErrorLevel,
	"warn":  logrus.WarnLevel,
	"info":  logrus.InfoLevel,
	"debug": logrus.DebugLevel,
}

func setLevel() {
	levelConf := Conf.LogLevel

	if levelConf == "" {
		levelConf = "info"
	}

	if level, ok := levels[levelConf]; ok {
		logrus.SetLevel(level)
	} else {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

type logFileWriter struct {
}

func (p *logFileWriter) Write(data []byte) (n int, err error) {
	appDir := GetAppDir()
	today := time.Now().Format("2006-01-02")
	logdir := appDir + "/log/log-" + today + ".log"
	file, err := os.OpenFile(logdir, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0600)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if file == nil {
		return 0, errors.New("file not opened")
	}

	n, e := file.Write(data)
	return n, e
}