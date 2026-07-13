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

// Package taskflowbinding holds the taskflow_binding_dag tasks. Where
// simple_dag's transform shows the minimal TaskFlow binding (one literal, one
// XCom), this Dag stresses the full argument surface: literals of every scalar
// type, an array literal, keyword arguments, a defaulted null, and XCom fan-in
// from two upstream Go tasks decoded into a strict struct and a typed slice.
package taskflowbinding

import (
	"fmt"
	"log/slog"
	"reflect"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Config is the object make_config returns as its XCom; combine declares the
// same struct as a parameter, so the round trip exercises strict struct
// decoding (an unknown or renamed key fails the task rather than silently
// zeroing a field).
type Config struct {
	Environment string `json:"environment"`
	Region      string `json:"region"`
	Debug       bool   `json:"debug"`
}

// MakeConfig pushes an object XCom that combine binds onto its Config parameter.
func MakeConfig(log *slog.Logger) (any, error) {
	cfg := Config{Environment: "production", Region: "eu-west-1", Debug: true}
	log.Info(
		"Pushing config",
		"environment",
		cfg.Environment,
		"region",
		cfg.Region,
		"debug",
		cfg.Debug,
	)
	return cfg, nil
}

// MakeNumbers pushes an array XCom that combine binds onto its []int parameter.
func MakeNumbers(log *slog.Logger) (any, error) {
	numbers := []int{1, 1, 2, 3, 5, 8}
	log.Info("Pushing numbers", "numbers", fmt.Sprint(numbers))
	return numbers, nil
}

// Combine receives every argument shape the stub Dag can express. The Python
// side calls it as
//
//	combine("summary", 3, 2.5, True, ["metrics", "hourly"],
//	        config=make_config(), numbers=make_numbers())
//
// so the bound values are fixed; any mismatch below is a binding regression
// and fails the task loudly. note is never passed and falls back to the stub's
// None default, arriving as a nil *string.
func Combine(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	name string,
	count int,
	ratio float64,
	enabled bool,
	tags []string,
	config Config,
	numbers []int,
	note *string,
) (any, error) {
	if name != "summary" || count != 3 || ratio != 2.5 || !enabled {
		return nil, fmt.Errorf(
			"scalar literals bound incorrectly: name=%q count=%d ratio=%v enabled=%v",
			name, count, ratio, enabled,
		)
	}
	if want := []string{"metrics", "hourly"}; !reflect.DeepEqual(tags, want) {
		return nil, fmt.Errorf("array literal bound incorrectly: tags=%v, want %v", tags, want)
	}
	if want := (Config{Environment: "production", Region: "eu-west-1", Debug: true}); config != want {
		return nil, fmt.Errorf("object XCom bound incorrectly: config=%+v, want %+v", config, want)
	}
	if want := []int{1, 1, 2, 3, 5, 8}; !reflect.DeepEqual(numbers, want) {
		return nil, fmt.Errorf("array XCom bound incorrectly: numbers=%v, want %v", numbers, want)
	}
	if note != nil {
		return nil, fmt.Errorf("defaulted None bound incorrectly: note=%q, want nil", *note)
	}

	sum := 0
	for _, n := range numbers {
		sum += n
	}
	log.InfoContext(ctx, "Bound TaskFlow arguments",
		"name", name,
		"count", count,
		"ratio", ratio,
		"enabled", enabled,
		"tags", fmt.Sprint(tags),
		"environment", config.Environment,
		"sum", sum,
	)
	return map[string]any{
		"name":          name,
		"count":         count,
		"ratio":         ratio,
		"enabled":       enabled,
		"tags":          tags,
		"environment":   config.Environment,
		"debug":         config.Debug,
		"sum":           sum,
		"note_was_null": note == nil,
	}, nil
}
