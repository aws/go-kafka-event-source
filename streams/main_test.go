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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

const kafkaProgramScript = "kafka_local/exec-kafka-script.sh"
const kafkaCleanupScript = "kafka_local/cleanup.sh"
const kafkaDownloadScript = "kafka_local/download-kafka.sh"
const kafkaWorkingDir = "kafka_local/kafka"

func TestMain(m *testing.M) {
	flag.Parse()
	// cleanup data logs in case we exited abnormally
	if err := exec.Command("sh", kafkaCleanupScript).Run(); err != nil {
		fmt.Println(err)
	}

	// download binaly distribution of kafka if necessary
	if err := exec.Command("sh", kafkaDownloadScript, kafkaWorkingDir).Run(); err != nil {
		fmt.Println(err)
	}

	// start zookeeper and broker asynchronously
	zookeeper := kafkaScriptCommand("zookeeper", "start")
	kafka := kafkaScriptCommand("kafka", "start")
	if err := zookeeper.Start(); err != nil {
		fmt.Println("zookeeper: ", err)
	}
	if err := kafka.Start(); err != nil {
		fmt.Println("broker: ", err)
	}

	// run our tests
	code := m.Run()

	// stop zookeeper and broker
	if err := kafkaScriptCommand("zookeeper", "stop").Run(); err != nil {
		fmt.Println("zookeeper: ", err)
	}
	if err := kafkaScriptCommand("kafka", "stop").Run(); err != nil {
		fmt.Println("kafka: ", err)
	}

	// give it a second then clean up data logs
	time.Sleep(time.Second)
	if err := exec.Command("sh", kafkaCleanupScript).Run(); err != nil {
		fmt.Println(err)
	}
	os.Exit(code)
}

func kafkaScriptCommand(program, command string) *exec.Cmd {
	return exec.Command("sh", kafkaProgramScript, kafkaWorkingDir, program, command)
}
