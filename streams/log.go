// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package streams

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type LogLevel int

const (
	LogLevelNone LogLevel = iota
	LogLevelTrace
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// Translate to LogLevel to kgo.LogLevel
func toKgoLoglevel(level LogLevel) kgo.LogLevel {
	switch level {
	// kgo does not define Trace, let's just say Trace == Debug
	case LogLevelTrace, LogLevelDebug:
		return kgo.LogLevelDebug
	case LogLevelInfo:
		return kgo.LogLevelInfo
	case LogLevelWarn:
		return kgo.LogLevelWarn
	case LogLevelError:
		return kgo.LogLevelError
	}
	return kgo.LogLevelNone
}

/*
Provides the interface needed by GKES to intergrate with your loggin mechanism. Example:

	 import (
		"mylogger"
		"github.com/aws/go-kafka-event-source/streams"
	 )

	 func main() {
		// GKES will emit log at whatever level is defined by NewLogger()
		// kgo will emit logs at LogLevelError
		streams.InitLogger(mylogger.NewLogger(), streams.LogLevelError)
	 }
*/
type Logger interface {
	Tracef(msg string, args ...any)
	Debugf(msg string, args ...any)
	Infof(msg string, args ...any)
	Warnf(msg string, args ...any)
	Errorf(msg string, args ...any)
}

// SimpleLogger implements Logger and writes to STDOUT. Good for development purposes.
type SimpleLogger LogLevel

type lazyTimeStampStringer struct{}

func (lazyTimeStampStringer) String() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

var lazyTimeStamp = lazyTimeStampStringer{}

func (sl SimpleLogger) Tracef(msg string, args ...any) {
	if LogLevelTrace >= LogLevel(sl) && LogLevel(sl) != LogLevelNone {
		fmt.Println(lazyTimeStamp, "[TRACE] -", fmt.Sprintf(msg, args...))
	}
}

func (sl SimpleLogger) Debugf(msg string, args ...any) {
	if LogLevelDebug >= LogLevel(sl) && LogLevel(sl) != LogLevelNone {
		fmt.Println(lazyTimeStamp, "[DEBUG] -", fmt.Sprintf(msg, args...))
	}
}

func (sl SimpleLogger) Infof(msg string, args ...any) {
	if LogLevelInfo >= LogLevel(sl) && LogLevel(sl) != LogLevelNone {
		fmt.Println(lazyTimeStamp, "[INFO] -", fmt.Sprintf(msg, args...))
	}
}

func (sl SimpleLogger) Warnf(msg string, args ...any) {
	if LogLevelWarn >= LogLevel(sl) && LogLevel(sl) != LogLevelNone {
		fmt.Println(lazyTimeStamp, "[WARN] -", fmt.Sprintf(msg, args...))
	}
}

func (sl SimpleLogger) Errorf(msg string, args ...any) {
	if LogLevelError >= LogLevel(sl) && LogLevel(sl) != LogLevelNone {
		fmt.Println(lazyTimeStamp, "[ERROR] -", fmt.Sprintf(msg, args...))
	}
}

// logWrapper allows you to utilize your own logger, but with a specific logging level for streams.
type logWrapper struct {
	level  LogLevel
	logger Logger
}

/*
WrapLogger allows GKES to emit logs at a higher level than your own Logger.
Useful if you need debug level logging for your own application, but want to cluuter your logs with gstream output.
Example:

	 import (
		"mylogger"
		"github.com/aws/go-kafka-event-source/streams"
	 )

	 func main() {
		// your application will emit logs at "Debug"
		// GKES will emit logs at LogLevelError
		// kgo will emit logs at LogLevelNone
		gkesLogger := streams.WrapLogger(mylogger.NewLogger("Debug"), streams.LogLevelError)
		streams.InitLogger(gkesLogger, streams.LogLevelNone)
	 }
*/
func WrapLogger(logger Logger, level LogLevel) Logger {
	return logWrapper{
		level:  level,
		logger: logger,
	}
}

func (lw logWrapper) Tracef(msg string, args ...any) {
	if LogLevelTrace >= lw.level && lw.level != LogLevelNone {
		lw.logger.Tracef(msg, args...)
	}
}

func (lw logWrapper) Debugf(msg string, args ...any) {
	if LogLevelDebug >= lw.level && lw.level != LogLevelNone {
		lw.logger.Debugf(msg, args...)
	}
}

func (lw logWrapper) Infof(msg string, args ...any) {
	if LogLevelInfo >= lw.level && lw.level != LogLevelNone {
		lw.logger.Infof(msg, args...)
	}
}

func (lw logWrapper) Warnf(msg string, args ...any) {
	if LogLevelWarn >= lw.level && lw.level != LogLevelNone {
		lw.logger.Warnf(msg, args...)
	}
}

func (lw logWrapper) Errorf(msg string, args ...any) {
	if LogLevelError >= lw.level && lw.level != LogLevelNone {
		lw.logger.Errorf(msg, args...)
	}
}

var log Logger = SimpleLogger(LogLevelError)
var kgoLogger kgo.Logger = kgoLogWrapper(kgo.LogLevelError)

type kgoLogWrapper kgo.LogLevel

func (klw kgoLogWrapper) Level() kgo.LogLevel {
	return kgo.LogLevel(klw)
}

func (klw kgoLogWrapper) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	switch level {
	case kgo.LogLevelDebug:
		log.Debugf(msg, keyvals...)
	case kgo.LogLevelInfo:
		log.Infof(msg, keyvals...)
	case kgo.LogLevelWarn:
		log.Warnf(msg, keyvals...)
	case kgo.LogLevelError:
		log.Errorf(msg, keyvals...)
	}
}

var oneLogger = sync.Once{}

/*
Initializes the GKES logger. `kafkaDriverLogLevel` defines the log level for the underlying kgo clients.
This call should be the first interaction with the GKES module. Subsequent calls will have no effect.
If never called, the default unitialized logger writes to STDOUT at LogLevelError for both GKES and kgo. Example:

	 import "github.com/aws/go-kafka-event-source/streams"

	 func main() {
		streams.InitLogger(streams.SimpleLogger(streams.LogLevelInfo), streams.LogLevelError)
		// ... initialize your application
	 }
*/
func InitLogger(l Logger, kafkaDriverLogLevel LogLevel) Logger {
	oneLogger.Do(func() {
		log = l
		kgoLogger = kgoLogWrapper(toKgoLoglevel(kafkaDriverLogLevel))
	})
	return log
}
