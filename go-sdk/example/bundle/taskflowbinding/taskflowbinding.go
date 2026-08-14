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

// Package taskflowbinding holds the taskflow_binding_dag tasks, one per shape
// of the Go SDK's TaskFlow argument binding. Where simple_dag's transform shows
// the minimal case, these cover the surface:
//
//   - ViaFlatArgs: positional binding at full stretch -- literals of every
//     scalar type, an array literal, keyword arguments, a defaulted null, and
//     XCom fan-in from two upstream Go tasks, decoded into a strict struct and
//     a typed slice.
//   - ViaStructNoTags: name binding with no tags, so each field claims the
//     argument spelled exactly like its Go field name.
//   - ViaStructArgTag: name binding driven by explicit `arg:` tags.
//   - ViaStructUnmatchedArg: a field no call argument names stays at its Go
//     zero value, and a captured stub default needs no field at all.
//   - ViaFlatMap: one dict decoded whole into a struct.
//   - ViaStructMap: one dict bound by name onto a struct's map field.
//   - ViaPlainMap: one dict decoded into a plain map, no struct involved.
//
// Every ViaStruct* call also binds MakeRegion's XCom onto its region field
// alongside a literal, so fields are exercised with both argument sources.
package taskflowbinding

import (
	"fmt"
	"log/slog"
	"reflect"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Config round-trips as an XCom into a struct parameter of the same type, so
// the strict decode is exercised: an unknown or renamed key fails the task.
type Config struct {
	Environment string `json:"environment"`
	Region      string `json:"region"`
	Debug       bool   `json:"debug"`
}

// MakeConfig pushes an object XCom that via_flat_args binds onto its Config parameter.
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

// MakeNumbers pushes an array XCom that via_flat_args binds onto its []int parameter.
func MakeNumbers(log *slog.Logger) (any, error) {
	numbers := []int{1, 1, 2, 3, 5, 8}
	log.Info("Pushing numbers", "numbers", fmt.Sprint(numbers))
	return numbers, nil
}

// MakeRegion pushes the string XCom every ViaStruct* task binds onto a field.
func MakeRegion(log *slog.Logger) (any, error) {
	region := "eu-west-1"
	log.Info("Pushing region", "region", region)
	return region, nil
}

// ViaFlatArgs is called as
//
//	via_flat_args("summary", 3, 2.5, True, ["metrics", "hourly"],
//	              config=make_config(), numbers=make_numbers())
//
// so every bound value is fixed and any mismatch below is a binding regression.
// note is never passed, so the stub's None default arrives as a nil *string.
func ViaFlatArgs(
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

// ViaStructNoTagsInput has no tags, so each field binds the argument spelled
// exactly like its Go field name -- hence the stub's capitalized parameters.
type ViaStructNoTagsInput struct {
	RegionCode string
	Threshold  float64
}

// ViaStructNoTags is called as
//
//	via_struct_no_tags(RegionCode=make_region(), Threshold=0.75)
//
// so RegionCode arrives via make_region's XCom and Threshold as a literal.
func ViaStructNoTags(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	input ViaStructNoTagsInput,
) (any, error) {
	if input.RegionCode != "eu-west-1" || input.Threshold != 0.75 {
		return nil, fmt.Errorf(
			"struct fields bound incorrectly: region_code=%q threshold=%v",
			input.RegionCode,
			input.Threshold,
		)
	}

	log.InfoContext(ctx, "Bound struct (no tags)",
		"region_code", input.RegionCode,
		"threshold", input.Threshold,
	)
	return map[string]any{
		"region_code": input.RegionCode,
		"threshold":   input.Threshold,
	}, nil
}

// ViaStructArgTagInput tags both fields: Region is genuinely renamed, proving
// the tag remaps rather than coincidentally matching, and Threshold needs its
// tag because an untagged field would only match "Threshold" verbatim.
type ViaStructArgTagInput struct {
	Region    string  `arg:"region_code"`
	Threshold float64 `arg:"threshold"`
}

// ViaStructArgTag is called as
//
//	via_struct_arg_tag(region_code=make_region(), threshold=0.75)
//
// so Region arrives via make_region's XCom and Threshold as a literal.
func ViaStructArgTag(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	input ViaStructArgTagInput,
) (any, error) {
	if input.Region != "eu-west-1" || input.Threshold != 0.75 {
		return nil, fmt.Errorf(
			"struct fields bound incorrectly: region=%q threshold=%v",
			input.Region,
			input.Threshold,
		)
	}

	log.InfoContext(ctx, "Bound struct (arg: tag)",
		"region", input.Region,
		"threshold", input.Threshold,
	)
	return map[string]any{
		"region":    input.Region,
		"threshold": input.Threshold,
	}, nil
}

// ViaStructUnmatchedArgInput demonstrates the mismatch tolerance in both
// directions. A field whose name has no corresponding TaskFlow call argument
// at all is left at its Go zero value rather than failing the task: Region
// binds normally, but Missing's arg name is never among this call's
// arguments -- conceptually, an unpassed keyword argument falling back to
// its default in a kwargs-style call. The reverse also holds: the stub's
// defaulted sample_rate parameter arrives marked from_default, so this
// struct is free not to mirror it (an explicitly passed argument no field
// claims would fail the task instead).
type ViaStructUnmatchedArgInput struct {
	Region  string `arg:"region_code"`
	Missing string `arg:"does_not_exist"`
}

// ViaStructUnmatchedArg is called as
//
//	via_struct_unmatched_arg(region_code=make_region())
//
// -- the stub declares region_code (bound from make_region's XCom) plus a
// defaulted sample_rate this struct deliberately omits, so Missing's arg
// name never appears among the call's arguments and stays at its Go zero
// value (""), while sample_rate's from_default entry goes unclaimed without
// failing the task.
func ViaStructUnmatchedArg(
	ctx sdk.TIRunContext, log *slog.Logger, input ViaStructUnmatchedArgInput,
) (any, error) {
	if input.Region != "eu-west-1" {
		return nil, fmt.Errorf("struct field bound incorrectly: region=%q", input.Region)
	}
	if input.Missing != "" {
		return nil, fmt.Errorf(
			"expected the unmatched field to stay at its Go zero value, got missing=%q",
			input.Missing,
		)
	}

	log.InfoContext(ctx, "Bound struct (unmatched arg)",
		"region", input.Region,
		"missing_was_empty", input.Missing == "",
	)
	return map[string]any{
		"region":            input.Region,
		"missing_was_empty": input.Missing == "",
	}, nil
}

// FlatMapConfig is decoded whole from a single dict argument -- the flat
// (whole-value) side of single-map injection. The TaskFlow call passes one
// object whose keys land on these fields; because the argument's name matches
// no field, the runtime decodes it whole rather than binding field-by-field.
type FlatMapConfig struct {
	Region string `json:"region"`
	Count  int    `json:"count"`
}

// ViaFlatMap is called as
//
//	via_flat_map(config={"region": "eu-west-1", "count": 3})
//
// The single "config" argument matches no FlatMapConfig field, so the whole
// dict is decoded into the struct -- one map bound flat into a struct.
func ViaFlatMap(
	ctx sdk.TIRunContext, log *slog.Logger, config FlatMapConfig,
) (any, error) {
	if config.Region != "eu-west-1" || config.Count != 3 {
		return nil, fmt.Errorf(
			"whole-value map bound incorrectly: region=%q count=%d",
			config.Region,
			config.Count,
		)
	}

	log.InfoContext(ctx, "Bound whole map into struct",
		"region", config.Region,
		"count", config.Count,
	)
	return map[string]any{"region": config.Region, "count": config.Count}, nil
}

// StructMapInput is a name-bound struct whose single field is itself a map, so
// one dict argument binds onto that field by name -- the struct-based side of
// single-map injection.
type StructMapInput struct {
	Payload map[string]any `arg:"payload"`
}

// ViaStructMap is called as
//
//	via_struct_map(payload={"region": "eu-west-1", "count": 3})
//
// The "payload" argument matches the Payload field by name, so the dict binds
// onto the map field -- one map bound by name onto a struct field.
func ViaStructMap(
	ctx sdk.TIRunContext, log *slog.Logger, input StructMapInput,
) (any, error) {
	region, _ := input.Payload["region"].(string)
	if region != "eu-west-1" {
		return nil, fmt.Errorf("map field bound incorrectly: payload=%v", input.Payload)
	}

	log.InfoContext(ctx, "Bound map onto struct field", "payload", fmt.Sprint(input.Payload))
	return map[string]any{"payload": input.Payload}, nil
}

// ViaPlainMap is called as
//
//	via_plain_map(labels={"team": "data", "tier": "gold"})
//
// The dict decodes straight into a typed map parameter, with no struct
// involved on the Go side at all.
func ViaPlainMap(
	ctx sdk.TIRunContext, log *slog.Logger, labels map[string]string,
) (any, error) {
	if labels["team"] != "data" || labels["tier"] != "gold" {
		return nil, fmt.Errorf("plain map bound incorrectly: labels=%v", labels)
	}

	log.InfoContext(ctx, "Bound dict into a plain map", "labels", fmt.Sprint(labels))
	return map[string]any{"team": labels["team"], "tier": labels["tier"]}, nil
}
