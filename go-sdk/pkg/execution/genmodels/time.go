// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package genmodels

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Time is a schema date-time. The supervisor's msgpack timestamp carries no
// zone and msgpack decodes it with time.Unix, i.e. in the worker's local zone,
// while Airflow means UTC.
//
// A string must carry an offset (RFC 3339). A naive one is rejected rather than
// read as UTC: it names no zone, and Airflow refuses to store naive datetimes.
//
// A defined type inherits none of time.Time's methods, so Time restates the
// ones it needs: EncodeMsgpack (else it encodes as an empty map), IsZero (so
// omitempty treats Time and *Time as it does time.Time: without it a Time is
// always dropped and a *Time to the zero time is kept) and String (for fmt).
type Time time.Time

// EncodeMsgpack encodes t as a msgpack timestamp.
func (t Time) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeTime(time.Time(t))
}

// IsZero reports whether t is the zero time, as time.Time.IsZero does.
func (t Time) IsZero() bool {
	return time.Time(t).IsZero()
}

// String formats t as time.Time.String does.
func (t Time) String() string {
	return time.Time(t).String()
}

// DecodeMsgpack decodes a msgpack timestamp into t in UTC.
func (t *Time) DecodeMsgpack(dec *msgpack.Decoder) error {
	v, err := dec.DecodeTime()
	if err != nil {
		return err
	}
	*t = Time(v.UTC())
	return nil
}

// TimePtr returns t as a *Time, for setting a nullable date-time field.
func TimePtr(t time.Time) *Time {
	v := Time(t)
	return &v
}
