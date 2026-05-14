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

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithBearerToken_AppliesRefreshedAPITokenFromResponse(t *testing.T) {
	var n int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		auth := r.Header.Get("Authorization")
		switch n {
		case 1:
			if auth != "Bearer first-secret" {
				t.Fatalf("first request: Authorization = %q, want Bearer first-secret", auth)
			}
			w.Header().Set("Refreshed-API-Token", "second-secret")
		case 2:
			if auth != "Bearer second-secret" {
				t.Fatalf("second request: Authorization = %q, want Bearer second-secret", auth)
			}
		default:
			t.Fatalf("unexpected extra request %d", n)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(ts.Close)

	root, err := NewClient(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	base := root.(*Client)

	withTok, err := base.WithBearerToken("first-secret")
	if err != nil {
		t.Fatal(err)
	}
	cli := withTok.(*Client)

	if _, err := cli.R().Get("/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.R().Get("/b"); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 requests, got %d", n)
	}
}
