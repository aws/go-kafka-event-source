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
	if err := exec.Command("sh", "kafka_local/download_kafka.sh", "kafka_local/kafka").Run(); err != nil {
		fmt.Println(err)
	}

	zookeeper := exec.Command("sh", "kafka_local/start_kafka_program.sh", "kafka_local/kafka", "zookeeper")
	kafka := exec.Command("sh", "kafka_local/start_kafka_program.sh", "kafka_local/kafka", "kafka")
	if err := zookeeper.Start(); err != nil {
		fmt.Println(err)
	}

	if err := kafka.Start(); err != nil {
		fmt.Println(err)
	}

	time.Sleep(5 * time.Second)
	m.Run()
	zookeeper.Process.Kill()
	if err := exec.Command("sh", "kafka_local/cleanup.sh").Run(); err != nil {
		fmt.Println(err)
	}
}
