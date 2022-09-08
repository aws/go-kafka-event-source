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
	"os/exec"
	"testing"
	"time"
)

func TestMain(m *testing.M) {

	if err := exec.Command("sh", "kafka_local/cleanup.sh").Run(); err != nil {
		fmt.Println(err)
	}
	kafkaScript := "kafka_local/exec-kafka-script.sh"
	workingDir := "kafka_local/kafka"

	if err := exec.Command("sh", "kafka_local/download_kafka.sh", workingDir).Run(); err != nil {
		fmt.Println(err)
	}

	zookeeper := exec.Command("sh", kafkaScript, workingDir, "zookeeper", "start")
	kafka := exec.Command("sh", kafkaScript, workingDir, "kafka", "start")
	if err := zookeeper.Start(); err != nil {
		fmt.Println("zookeeper: ", err)
	}

	if err := kafka.Start(); err != nil {
		fmt.Println("broker: ", err)
	}

	m.Run()

	if err := exec.Command("sh", kafkaScript, workingDir, "zookeeper", "stop").Run(); err != nil {
		fmt.Println("zookeeper: ", err)
	}

	if err := exec.Command("sh", kafkaScript, workingDir, "kafka", "stop").Run(); err != nil {
		fmt.Println("kafka: ", err)
	}

	time.Sleep(time.Second)
	if err := exec.Command("sh", "kafka_local/cleanup.sh").Run(); err != nil {
		fmt.Println(err)
	}
}
