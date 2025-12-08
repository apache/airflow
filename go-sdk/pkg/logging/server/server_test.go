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

package server

import (
	"context"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
}

func (s *ServerTestSuite) TestContainsDotFile() {
	tests := []struct {
		input    string
		expected bool
	}{
		{"foo/bar/baz", false},
		{".foo/bar", true},
		{"foo/.bar/baz", true},
		{"foo/bar/.baz", true},
		{".", true},
		{"./foo", true},
		{"", false},
	}
	for _, tt := range tests {
		s.Equal(tt.expected, containsDotFile(tt.input), "input: %s", tt.input)
	}
}

func (s *ServerTestSuite) TestDotFileHidingFileReaddir_FiltersDotFiles() {
	// Use MapFS for backing files
	fsMap := fstest.MapFS{
		"foo.txt":     {Data: []byte("foo")},
		".hidden.txt": {Data: []byte("hidden")},
		"bar.log":     {Data: []byte("bar")},
		".dotfile":    {Data: []byte("dot")},
	}
	root, err := fs.Sub(fsMap, ".")
	s.Require().NoError(err)
	// Open all files in root
	files, err := fs.ReadDir(root, ".")
	s.Require().NoError(err)

	// Convert DirEntry to FileInfo
	fileInfos := make([]fs.FileInfo, 0, len(files))
	for _, f := range files {
		info, err := f.Info()
		s.Require().NoError(err)
		fileInfos = append(fileInfos, info)
	}
	df := dotFileHidingFile{mockHTTPFile{files: fileInfos}}
	filtered, err := df.Readdir(-1)
	s.NoError(err)
	names := []string{}
	for _, f := range filtered {
		names = append(names, f.Name())
	}
	s.Equal([]string{"bar.log", "foo.txt"}, names)
}

type mockHTTPFile struct {
	http.File
	files []fs.FileInfo
}

func (m mockHTTPFile) Readdir(n int) ([]fs.FileInfo, error) {
	return m.files, nil
}

func (s *ServerTestSuite) TestDotFileHidingFileSystem_Open_RejectsDotFiles() {
	fsMap := fstest.MapFS{
		".hidden/file.txt": {Data: []byte("secret")},
	}
	fsys := dotFileHidingFileSystem{http.FS(fsMap)}
	_, err := fsys.Open(".hidden/file.txt")
	s.ErrorIs(err, fs.ErrPermission)
}

func (s *ServerTestSuite) TestDotFileHidingFileSystem_Open_DelegatesForNormalFile() {
	fsMap := fstest.MapFS{
		"normal.txt": {Data: []byte("normal")},
	}
	fsys := dotFileHidingFileSystem{http.FS(fsMap)}
	f, err := fsys.Open("normal.txt")
	s.NoError(err)
	_, ok := f.(dotFileHidingFile)
	s.True(ok)
}

func (s *ServerTestSuite) TestMakeJWTParser_ValidAlgorithm() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	parser, err := makeJWTParser(cfg)
	s.NoError(err)
	s.NotNil(parser)
}

func (s *ServerTestSuite) TestMakeJWTParser_InvalidAlgorithm() {
	cfg := DefaultConfig
	cfg.Algorithm = "invalid-algo"
	_, err := makeJWTParser(cfg)
	s.Error(err)
}

func (s *ServerTestSuite) TestNewLogServer_MissingSecretKey() {
	cfg := DefaultConfig
	cfg.SecretKey = ""
	_, err := NewLogServer(nil, cfg)
	s.Error(err)
}

func (s *ServerTestSuite) TestNewFromConfig_MissingSecretKey() {
	v := viper.New()
	v.Set("logging.base_log_folder", ".")
	v.Set("logging.secret_key", "")
	_, err := NewFromConfig(v)
	s.Error(err)
}

func (s *ServerTestSuite) TestNewFromConfig_AllFields() {
	v := viper.New()
	v.Set("logging.base_log_folder", ".")
	v.Set("logging.secret_key", "mysecret")
	v.Set("logging.worker_log_server_port", 12345)
	v.Set("logging.clock_leeway", "10s")
	v.Set("logging.audiences", []string{"aud1", "aud2"})
	v.Set("logging.algorithm", "HS256")
	srv, err := NewFromConfig(v)
	s.NoError(err)
	s.NotNil(srv)
}

func (s *ServerTestSuite) TestLogServer_ServeLog_ForwardsToFileServer() {
	fsMap := fstest.MapFS{
		"foo.log": {Data: []byte("logdata")},
	}
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	cfg.BaseLogFolder = "."
	srv, err := NewLogServer(slog.Default(), cfg)
	s.NoError(err)
	// override fileServer and fs to use MapFS
	srv.fs = dotFileHidingFileSystem{http.FS(fsMap)}
	srv.fileServer = http.FileServer(srv.fs)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo.log", nil)
	srv.ServeLog(rec, req)
	s.Contains([]int{http.StatusOK, http.StatusNotFound}, rec.Code)
}

func (s *ServerTestSuite) TestValidateToken_MissingAuthHeader() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	srv, err := NewLogServer(slog.Default(), cfg)
	s.NoError(err)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo.log", nil)
	handler := srv.validateToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Fail("should not call next")
	}))
	handler.ServeHTTP(rec, req)
	s.Equal(http.StatusForbidden, rec.Code)
	s.Contains(rec.Body.String(), "Authorization header missing")
}

func (s *ServerTestSuite) TestValidateToken_InvalidToken() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	srv, err := NewLogServer(slog.Default(), cfg)
	s.NoError(err)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo.log", nil)
	req.Header.Set("Authorization", "Bearer invalidtoken")
	handler := srv.validateToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Fail("should not call next")
	}))
	handler.ServeHTTP(rec, req)
	s.Equal(http.StatusForbidden, rec.Code)
	s.Contains(rec.Body.String(), "Invalid Authorization header")
}

func (s *ServerTestSuite) TestValidateToken_ValidTokenWrongFilename() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	srv, err := NewLogServer(slog.Default(), cfg)
	s.NoError(err)

	claims := jwt.MapClaims{
		"filename": "/other.log",
		"aud":      DefaultAudience,
		"exp":      time.Now().Add(1 * time.Minute).Unix(),
	}
	token := jwt.NewWithClaims(jwt.GetSigningMethod(cfg.Algorithm), claims)
	signed, _ := token.SignedString([]byte(cfg.SecretKey))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo.log", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	handler := srv.validateToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Fail("should not call next")
	}))
	handler.ServeHTTP(rec, req)
	s.Equal(http.StatusForbidden, rec.Code)
	s.Contains(rec.Body.String(), "Invalid Authorization header")
}

func (s *ServerTestSuite) TestValidateToken_ValidToken_CallsNext() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	srv, err := NewLogServer(slog.Default(), cfg)
	s.NoError(err)

	claims := jwt.MapClaims{
		"filename": "/foo.log",
		"aud":      DefaultAudience,
		"exp":      time.Now().Add(1 * time.Minute).Unix(),
	}
	token := jwt.NewWithClaims(jwt.GetSigningMethod(cfg.Algorithm), claims)
	signed, _ := token.SignedString([]byte(cfg.SecretKey))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo.log", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	called := false
	handler := srv.validateToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(rec, req)
	s.True(called)
	s.Equal(http.StatusOK, rec.Code)
}

// TestRun starts the server on an available port and verifies it can be shut down via context cancellation.
func (s *ServerTestSuite) TestLogServer_Run_StartsAndShutsDownCleanly() {
	cfg := DefaultConfig
	cfg.SecretKey = "testkey"
	cfg.Port = 0 // 0 means choose any available port

	srv, err := NewLogServer(slog.Default(), cfg)
	s.Require().NoError(err)

	// Override handler to prevent actual file serving and simplify test
	srv.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Listen on a random port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error)
	go func() {
		runErrCh <- srv.Serve(ctx, 2*time.Second, ln)
	}()

	// Connect to the server to ensure it's up
	client := &http.Client{Timeout: 1 * time.Second}

	url := "http://" + ln.Addr().String()
	var resp *http.Response
	for i := 0; i < 10; i++ {
		resp, err = client.Get(url)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.Require().NoError(err)
	s.Equal(http.StatusOK, resp.StatusCode)
	// Now shut down
	cancel()
	// Wait for shutdown
	select {
	case err := <-runErrCh:
		s.NoError(err)
	case <-time.After(5 * time.Second):
		s.Fail("server did not shut down in time")
	}
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
