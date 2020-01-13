// Copyright 2019 The JIMDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cblog

import (
	"fmt"
	"os"
)

var blog *baudLog

func NewBaudLog(dir, module, level string, toConsole bool) *baudLog {
	var l loggingT
	l.dir = dir
	l.module = module
	l.alsoToStderr = toConsole
	var ok bool
	if l.outputLevel, ok = severityByName(level); !ok {
		panic("Unknown output log level")
	}
	ToInit(&l)
	return &baudLog{l: &l}
}

type baudLog struct {
	l *loggingT
}

func (l *baudLog) IsDebugEnabled() bool {
	return l.l.outputLevel == debugLog
}

func (l *baudLog) IsInfoEnabled() bool {
	return int(l.l.outputLevel) <= INFO
}

func (l *baudLog) IsWarnEnabled() bool {
	return int(l.l.outputLevel) <= WARN
}

func (l *baudLog) Error(format string, arg ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, arg...)
	}
}

func (l *baudLog) Info(format string, arg ...interface{}) {
	if infoLog >= l.l.outputLevel {
		l.l.printDepth(infoLog, 1, format, arg...)
	}
}

func (l *baudLog) Debug(format string, arg ...interface{}) {
	if debugLog >= l.l.outputLevel {
		l.l.printDepth(debugLog, 1, format, arg...)
	}
}

func (l *baudLog) Warn(format string, arg ...interface{}) {
	if warningLog >= l.l.outputLevel {
		l.l.printDepth(warningLog, 1, format, arg...)
	}
}

func (l *baudLog) Panic(format string, v ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, v...)
	}
	panic(fmt.Sprintf(format, v...))
}

func (l *baudLog) Fault(format string, v ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, v...)
	}
	os.Exit(-1)
}

func (l *baudLog) Flush() {
	l.l.lockAndFlushAll()
}

func (l *baudLog) SetLogLevel(levelStr string) {
	var ok bool
	if l.l.outputLevel, ok = severityByName(levelStr); !ok {
		l.l.outputLevel = infoLog
	}
}
