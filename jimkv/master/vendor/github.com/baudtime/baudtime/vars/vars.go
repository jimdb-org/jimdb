package vars

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	osutil "github.com/baudtime/baudtime/util/os"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	AppName        string
	CpuProfile     string
	MemProfile     string
	ConfigFilePath string
	Logger         log.Logger
	LogWriter      io.Writer
	LocalIP        string
	PageSize       = os.Getpagesize()
)

func Init(appName string) {
	AppName = appName

	logDir := flag.String("log-dir", "", "logs will be written to this directory")
	logLevel := flag.String("log-level", "warn", "log level")
	flag.StringVar(&ConfigFilePath, "config", appName+".toml", "configure file path")
	flag.StringVar(&CpuProfile, "cpu-prof", "", "write cpu profile to file")
	flag.StringVar(&MemProfile, "mem-prof", "", "write memory profile to file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] \n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var err error

	if debugIP, found := os.LookupEnv("debugIP"); found {
		LocalIP = debugIP
	} else {
		LocalIP, err = osutil.GetLocalIP()
		if err != nil {
			panic(err)
		}
	}

	if ip := net.ParseIP(LocalIP); ip == nil {
		panic("invalid ip address")
	}

	if len(*logDir) == 0 {
		LogWriter = os.Stdout
	} else {
		LogWriter = &lumberjack.Logger{
			Filename:   *logDir + "/" + appName + ".log",
			MaxSize:    512, // megabytes
			MaxBackups: 3,
			MaxAge:     28, //days
		}
	}

	var levelOpt level.Option
	switch *logLevel {
	case "error":
		levelOpt = level.AllowError()
	case "warn":
		levelOpt = level.AllowWarn()
	case "info":
		levelOpt = level.AllowInfo()
	case "debug":
		levelOpt = level.AllowDebug()
	default:
		levelOpt = level.AllowWarn()
	}

	Logger = level.NewFilter(log.NewLogfmtLogger(LogWriter), levelOpt)
	Logger = log.With(Logger, "time", log.TimestampFormat(time.Now, "2006-01-02T15:04:05.999999999"), "caller", log.DefaultCaller)

	if err = LoadConfig(ConfigFilePath); err != nil {
		panic(err)
	}
}
