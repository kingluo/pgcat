package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/jackc/pgx/v4"
	"github.com/kingluo/pgcat"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	yaml "gopkg.in/yaml.v2"
)

type pgcatCfg struct {
	Host               string   `yaml:"host"`
	Port               uint16   `yaml:"port"`
	Password           string   `yaml:"password"`
	Databases          []string `yaml:"databases"`
	LogLevel           string   `yaml:"loglevel,omitempty"`
	PgxLogLevel        string   `yaml:"pgxloglevel,omitempty"`
	LogFile            string   `yaml:"logfile,omitempty"`
	MaxSyncWorkers     int      `yaml:"max_sync_workers,omitempty"`
	AdminListenAddress string   `yaml:"admin_listen_address,omitempty"`
	ClientMinMessages  string   `yaml:"client_min_messages,omitempty"`
}

func readConfig(cfgFilePath string) *pgcatCfg {
	cfgFile, err := os.Open(cfgFilePath)
	defer cfgFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	cfg := &pgcatCfg{}
	if err := yaml.NewDecoder(cfgFile).Decode(cfg); err != nil {
		log.Fatal(err)
	}
	if cfg.PgxLogLevel == "" {
		cfg.PgxLogLevel = "none"
	}
	pgxLogLevel, err = pgx.LogLevelFromString(cfg.PgxLogLevel)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.MaxSyncWorkers == 0 {
		cfg.MaxSyncWorkers = 1
	}
	return cfg
}

var gLog *zap.SugaredLogger
var gAtomicLevel = zap.NewAtomicLevel()
var pgxLogLevel pgx.LogLevel
var gFileWriter *fileWriter

type fileWriter struct {
	fileName string
	*os.File
	reOpen int32
}

func newFileWriter(name string) *fileWriter {
	f := &fileWriter{fileName: name}
	f.reopen()
	return f
}

func (f *fileWriter) reopen() {
	if f.fileName != "" {
		var err error
		if f.File != nil {
			if err := f.Close(); err != nil {
				gLog.Fatal(err)
			}
		}
		f.File, err = os.OpenFile(f.fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			gLog.Fatal(err)
		}
	} else {
		f.File = os.Stdout
	}
}

func (f *fileWriter) Write(b []byte) (n int, err error) {
	if atomic.SwapInt32(&f.reOpen, 0) == 1 {
		f.reopen()
	}
	return f.File.Write(b)
}

func (f *fileWriter) SetReOpen() {
	atomic.StoreInt32(&f.reOpen, 1)
}

func newLogger(atom zap.AtomicLevel, sw zapcore.WriteSyncer,
	options ...zap.Option) *zap.SugaredLogger {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	options = append(options, zap.AddCaller())
	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.Lock(sw),
		atom,
	), options...)

	return zapLogger.Sugar()
}

var currentCfg *pgcatCfg
var cfgFilePath = flag.String("c", "pgcat.yml", "pgcat config file")

var reloadMux sync.Mutex
var dbRunStates = make(map[string]*pgcat.DbRunState)

func runDB(cfg *pgcatCfg, db string) {
	st := &pgcat.DbRunState{
		Host:           cfg.Host,
		Port:           cfg.Port,
		Password:       cfg.Password,
		DB:             db,
		MaxSyncWorkers: cfg.MaxSyncWorkers,
		CloseCh:        make(chan struct{}),
		StopCh:         make(chan struct{}),
		SugaredLogger: zap.S().With(
			"host", cfg.Host,
			"port", cfg.Port,
			"db", db,
		),
		PgxLogLevel:       pgxLogLevel,
		ClientMinMessages: cfg.ClientMinMessages,
	}
	dbRunStates[db] = st
	go pgcat.RunDatabase(st)
}

func reload(w http.ResponseWriter, r *http.Request) {
	gLog.Info("reload config")

	reloadMux.Lock()
	defer reloadMux.Unlock()

	newCfg := readConfig(*cfgFilePath)

	if newCfg.LogLevel != currentCfg.LogLevel {
		gAtomicLevel.UnmarshalText([]byte(newCfg.LogLevel))
	}

	if newCfg.LogFile != currentCfg.LogFile {
		gFileWriter.fileName = newCfg.LogFile
		gFileWriter.SetReOpen()
	}

	if newCfg.Host != currentCfg.Host ||
		newCfg.Port != currentCfg.Port ||
		newCfg.Password != currentCfg.Password ||
		newCfg.MaxSyncWorkers != currentCfg.MaxSyncWorkers {
		for _, st := range dbRunStates {
			close(st.CloseCh)
			<-st.StopCh
		}
		dbRunStates = make(map[string]*pgcat.DbRunState)
		for _, db := range newCfg.Databases {
			runDB(newCfg, db)
		}
	} else {
		// start new databases
		for _, db := range newCfg.Databases {
			if _, ok := dbRunStates[db]; !ok {
				runDB(newCfg, db)
			}
		}

		// stop removed databases
		var removed []string
		for _, st := range dbRunStates {
			found := false
			for _, db := range newCfg.Databases {
				if st.DB == db {
					found = true
					break
				}
			}
			if !found {
				removed = append(removed, st.DB)
				close(st.CloseCh)
				<-st.StopCh
			}
		}
		for _, db := range removed {
			delete(dbRunStates, db)
		}
	}

	currentCfg = newCfg
	fmt.Fprintln(w, "ok")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	currentCfg = readConfig(*cfgFilePath)

	// setup log
	gAtomicLevel.UnmarshalText([]byte(currentCfg.LogLevel))
	gFileWriter = newFileWriter(currentCfg.LogFile)
	gLog = newLogger(gAtomicLevel, gFileWriter)
	zap.ReplaceGlobals(gLog.Desugar())

	gLog.Infof("start pgcat...")

	// run all db
	defer func() {
		reloadMux.Lock()
		defer reloadMux.Unlock()
		for _, st := range dbRunStates {
			close(st.CloseCh)
			<-st.StopCh
		}
	}()

	for _, db := range currentCfg.Databases {
		runDB(currentCfg, db)
	}

	// setup the web api
	if currentCfg.AdminListenAddress != "" {
		http.HandleFunc("/reload", reload)
		http.HandleFunc("/rotate", func(w http.ResponseWriter, r *http.Request) {
			gFileWriter.SetReOpen()
			fmt.Fprintln(w, "ok")
		})
		go http.ListenAndServe(currentCfg.AdminListenAddress, nil)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt, syscall.SIGINT)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT)
	<-c
}
