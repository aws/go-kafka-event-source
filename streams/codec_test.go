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
	"bytes"
	"testing"
)

func TestLexoIntCodec(t *testing.T) {
	a := bytes.NewBuffer(nil)
	b := bytes.NewBuffer(nil)
	c := bytes.NewBuffer(nil)
	d := bytes.NewBuffer(nil)
	LexoInt64Codec.Encode(a, -2)
	LexoInt64Codec.Encode(b, 1)
	LexoInt64Codec.Encode(c, 10)
	LexoInt64Codec.Encode(d, -4)
	if bytes.Compare(a.Bytes(), b.Bytes()) >= 0 {
		t.Errorf("invalid lexo compare %d, %d", -2, 1)
	}

	if bytes.Compare(b.Bytes(), c.Bytes()) >= 0 {
		t.Errorf("invalid lexo compare %d, %d", 1, 10)
	}

	if bytes.Compare(d.Bytes(), a.Bytes()) >= 0 {
		t.Errorf("invalid lexo compare %d, %d", -4, -2)
	}
}

func TestLexoIntCodecDecode(t *testing.T) {
	a := bytes.NewBuffer(nil)
	b := bytes.NewBuffer(nil)
	c := bytes.NewBuffer(nil)
	d := bytes.NewBuffer(nil)
	LexoInt64Codec.Encode(a, -2)
	LexoInt64Codec.Encode(b, 1)
	LexoInt64Codec.Encode(c, 10)
	LexoInt64Codec.Encode(d, -4)

	if v, _ := LexoInt64Codec.Decode(a.Bytes()); v != -2 {
		t.Errorf("invalid lexo decode. actual: %d, expected: %d", v, -2)
	}

	if v, _ := LexoInt64Codec.Decode(b.Bytes()); v != 1 {
		t.Errorf("invalid lexo decode. actual: %d, expected: %d", v, 1)
	}

	if v, _ := LexoInt64Codec.Decode(c.Bytes()); v != 10 {
		t.Errorf("invalid lexo decode. actual: %d, expected: %d", v, 10)
	}

	if v, _ := LexoInt64Codec.Decode(d.Bytes()); v != -4 {
		t.Errorf("invalid lexo decode. actual: %d, expected: %d", v, -4)
	}

}
