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

package execution

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// SupervisorSchemaVersion is the dated AIP-72 supervisor wire-schema version
// (YYYY-MM-DD) this SDK's coordinator protocol is compiled against. It must
// match the "api_version" of the schema the models are generated from, and is
// reported in a bundle's airflow-metadata manifest as
// sdk.supervisor_schema_version so the supervisor can down/upgrade messages to
// a shape the bundle understands.
const SupervisorSchemaVersion = "2027-02-28"

// The message-type discriminator strings (genmodels.Type*) are generated from the
// schema's "type" consts in discriminators.gen.go; outbound messages stamp the
// value and inbound dispatch matches it, so nothing here hand-writes a wire string.

// typeEnvelope peeks only the "type" discriminator from a raw body; msgpack
// ignores unknown map keys, so it decodes against any message body.
type typeEnvelope struct {
	Type string `msgpack:"type"`
}

// peekBodyType returns the "type" discriminator of a raw msgpack body, or "" if
// the body is nil or carries no type.
func peekBodyType(raw msgpack.RawMessage) string {
	if isNilRaw(raw) {
		return ""
	}
	var env typeEnvelope
	if err := msgpack.Unmarshal(raw, &env); err != nil {
		return ""
	}
	return env.Type
}

// decodeIncomingBody decodes a raw msgpack body into the concrete genmodels
// message named by its "type" discriminator, for the types that can arrive
// unsolicited as the supervisor's first frame.
func decodeIncomingBody(raw msgpack.RawMessage) (any, error) {
	if isNilRaw(raw) {
		return nil, nil
	}
	typ := peekBodyType(raw)
	switch typ {
	case genmodels.TypeStartupDetails:
		var msg genmodels.StartupDetails
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("decoding StartupDetails: %w", err)
		}
		if err := validateStartupTI(msg.TI); err != nil {
			return nil, fmt.Errorf("decoding StartupDetails: %w", err)
		}
		return &msg, nil
	case genmodels.TypeDagFileParseRequest:
		var msg genmodels.DagFileParseRequest
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("decoding DagFileParseRequest: %w", err)
		}
		return &msg, nil
	case genmodels.TypeErrorResponse:
		var msg genmodels.ErrorResponse
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("decoding ErrorResponse: %w", err)
		}
		return &msg, nil
	default:
		return nil, fmt.Errorf("unknown message type %q", typ)
	}
}

// validateStartupTI fails fast when StartupDetails omits a required TaskInstance
// field: msgpack decodes a missing key to the Go zero value, which would
// otherwise silently run the task with e.g. try_number 0.
func validateStartupTI(ti genmodels.TaskInstance) error {
	switch {
	case ti.ID == "":
		return fmt.Errorf("ti.id: missing or empty")
	case ti.DagID == "":
		return fmt.Errorf("ti.dag_id: missing or empty")
	case ti.TaskID == "":
		return fmt.Errorf("ti.task_id: missing or empty")
	case ti.RunID == "":
		return fmt.Errorf("ti.run_id: missing or empty")
	case ti.TryNumber < 1:
		return fmt.Errorf("ti.try_number: missing or invalid (%d)", ti.TryNumber)
	}
	return nil
}

// decodeBody decodes a raw msgpack response body into dst, a pointer to the
// genmodels result type the caller expects.
func decodeBody(raw msgpack.RawMessage, dst any) error {
	if isNilRaw(raw) {
		return fmt.Errorf("empty response body")
	}
	return msgpack.Unmarshal(raw, dst)
}

// apiErrorFromFrame returns the supervisor error carried by a frame, or nil if
// it is not an error reply. An error arrives either as the third element of a
// 3-tuple frame (frame.Err) or as a 2-tuple body whose "type" is "ErrorResponse".
func apiErrorFromFrame(f IncomingFrame) *APIError {
	var raw msgpack.RawMessage
	switch {
	case !isNilRaw(f.Err):
		raw = f.Err
	case peekBodyType(f.Body) == genmodels.TypeErrorResponse:
		raw = f.Body
	default:
		return nil
	}

	var resp genmodels.ErrorResponse
	if err := msgpack.Unmarshal(raw, &resp); err != nil {
		// detail is off-contract (schema types it as object|null); still recover
		// the error code so callers get the typed error, not a generic one.
		var code struct {
			Error genmodels.ErrorType `msgpack:"error"`
		}
		_ = msgpack.Unmarshal(raw, &code)
		errCode := string(code.Error)
		if errCode == "" {
			errCode = string(genmodels.ErrorTypeGENERICERROR)
		}
		return &APIError{
			Err:    errCode,
			Detail: fmt.Sprintf("undecodable error frame: %v", err),
		}
	}
	var detail any
	if resp.Detail != nil {
		detail = map[string]any(*resp.Detail)
	}
	return &APIError{Err: string(resp.Error), Detail: detail}
}
