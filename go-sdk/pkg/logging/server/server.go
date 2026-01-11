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

// Package server implements an HTTP server to make in-progress task logs available to the Airflow UI
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sloghttp "github.com/samber/slog-http"
	"github.com/spf13/viper"
)

const (
	DefaultAudience            = "task-instance-logs"
	DefaultAlgorithm           = "HS512"
	DefaultWorkerLogServerPort = 8793
)

// We need it to be a pointer, so it can't be a const.

var DefaultClockLeeway = 30 * time.Second

type Config struct {
	BaseLogFolder string `mapstructure:"base_log_folder"`
	Port          int    `mapstructure:"port"`

	SecretKey   string         `mapstructure:"secret_key"`
	ClockLeeway *time.Duration `mapstructure:"clock_leeway"`
	Algorithm   string         `mapstructure:"algorithm"`
	Audiences   []string       `mapstructure:"audiences"`
}

var DefaultConfig = Config{
	BaseLogFolder: ".",
	Port:          DefaultWorkerLogServerPort,
	ClockLeeway:   &DefaultClockLeeway,
	Algorithm:     DefaultAlgorithm,
	Audiences:     []string{DefaultAudience},
}

type LogServer struct {
	server *http.Server
	logger *slog.Logger

	jwtParser  *jwt.Parser
	jwtKeyFunc func(*jwt.Token) (any, error)
	fileServer http.Handler
	fs         dotFileHidingFileSystem
}

func init() {
	sloghttp.RequestIDHeaderKey = "Correlation-ID"
}

func NewFromConfig(v *viper.Viper) (*LogServer, error) {
	// TODO: Unmarshal doesn't seem to work with configs from env. Something like needs binding? first?
	cfg := DefaultConfig

	cfg.BaseLogFolder = v.GetString("logging.base_log_folder")
	cfg.SecretKey = v.GetString("logging.secret_key")

	if v.IsSet("logging.worker_log_server_port") {
		cfg.Port = v.GetInt("logging.worker_log_server_port")
	}

	if v.IsSet("logging.clock_leeway") {
		if v.GetString("logging.clock_leeway") == "" {
			cfg.ClockLeeway = nil
		} else {
			leeway := v.GetDuration("logging.clock_leeway")
			cfg.ClockLeeway = &leeway
		}
	}

	if v.IsSet("logging.audiences") {
		cfg.Audiences = v.GetStringSlice("logging.audiences")
	}

	if v.IsSet("logging.algorithm") {
		cfg.Algorithm = v.GetString("logging.algorithm")
	}

	return NewLogServer(nil, cfg)
}

func NewLogServer(logger *slog.Logger, cfg Config) (*LogServer, error) {
	mux := http.NewServeMux()

	if cfg.SecretKey == "" {
		return nil, errors.New("logging.secret_key config option must be provided")
	}

	if logger == nil {
		logger = slog.Default().With("logger", "pkg.logging.server")
	}
	parser, err := makeJWTParser(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create log HTTP server: %w", err)
	}

	handler := sloghttp.Recovery(mux)
	handler = sloghttp.NewWithFilters(
		logger.WithGroup("http"),
		sloghttp.IgnorePath("/favicon.ico"),
	)(
		handler,
	)

	fs := dotFileHidingFileSystem{http.Dir(cfg.BaseLogFolder)}

	server := &LogServer{
		server: &http.Server{
			Handler: handler,
			Addr:    fmt.Sprintf(":%d", cfg.Port),
		},
		logger: logger,

		jwtParser:  parser,
		jwtKeyFunc: func(*jwt.Token) (any, error) { return []byte(cfg.SecretKey), nil },
		fs:         fs,
		fileServer: http.FileServer(fs),
	}

	mux.Handle("GET /favicon.ico", http.NotFoundHandler())
	mux.Handle("GET /log/", http.StripPrefix(
		"/log/",
		server.validateToken(http.HandlerFunc(server.ServeLog)),
	))

	return server, nil
}

func makeJWTParser(cfg Config) (*jwt.Parser, error) {
	if cfg.Algorithm == "" {
		cfg.Algorithm = DefaultAlgorithm
	}

	if !slices.Contains(jwt.GetAlgorithms(), cfg.Algorithm) {
		return nil, fmt.Errorf("unknown jwt signing algorithm %q", cfg.Algorithm)
	}

	opts := []jwt.ParserOption{
		jwt.WithAudience(cfg.Audiences...),
		jwt.WithValidMethods([]string{cfg.Algorithm}),
		jwt.WithExpirationRequired(),
	}
	if cfg.ClockLeeway != nil {
		opts = append(opts, jwt.WithLeeway(*cfg.ClockLeeway))
	}
	parser := jwt.NewParser(opts...)
	return parser, nil
}

func (l *LogServer) ListenAndServe(ctx context.Context, shutdownTimeout time.Duration) error {
	// This is what l.server.ListenAndServe does, but we copy it here so we can call Serve directly in tests
	addr := l.server.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return l.Serve(ctx, shutdownTimeout, ln)
}

func (l *LogServer) Serve(
	ctx context.Context,
	shutdownTimeout time.Duration,
	ln net.Listener,
) error {
	uncancelalbleCtx := context.WithoutCancel(ctx)
	idleConnsClosed := make(chan struct{})
	go func() {
		// Wait until the original context passed to `Run` is done
		<-ctx.Done()

		l.logger.Info("Shutting down log server")

		shutdownCtx, cancel := context.WithTimeout(uncancelalbleCtx, shutdownTimeout)
		l.server.Shutdown(shutdownCtx)

		close(idleConnsClosed)
		cancel() // To avoid a context leak, we always close the context when we are done
	}()
	l.server.BaseContext = func(sock net.Listener) context.Context {
		l.logger.Info("Listening for logs", "addr", "http://"+sock.Addr().String())
		return uncancelalbleCtx
	}

	if err := l.server.Serve(ln); err != http.ErrServerClosed {
		// Error starting or closing listener:
		return err
	}

	<-idleConnsClosed
	return nil
}

func (l *LogServer) validateToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")

		if auth == "" {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("Authorization header missing"))
			return
		}

		token, err := l.jwtParser.Parse(strings.TrimPrefix(auth, "Bearer "), l.jwtKeyFunc)
		if err != nil {
			l.logger.Error(
				"Token validation failed",
				slog.Group("http", slog.String("id", sloghttp.GetRequestID(r))),
				"err",
				err,
			)
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("Invalid Authorization header"))
			return
		}

		fnameClaim, ok := token.Claims.(jwt.MapClaims)["filename"].(string)
		if !ok || fnameClaim != r.URL.Path {
			l.logger.Error(
				"Claim is for a different path than the URL",
				"fnClaim",
				fnameClaim,
				"r.URL.Path",
				r.URL.Path,
			)
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("Invalid Authorization header"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (l *LogServer) ServeLog(w http.ResponseWriter, r *http.Request) {
	w.Header().Add(sloghttp.RequestIDHeaderKey, sloghttp.GetRequestID(r))
	// TODO: validate token
	l.fileServer.ServeHTTP(w, r)
}

// containsDotFile reports whether name contains a path element starting with a period.
// The name is assumed to be a delimited by forward slashes, as guaranteed
// by the http.FileSystem interface.
func containsDotFile(name string) bool {
	parts := strings.Split(name, "/")
	for _, part := range parts {
		if strings.HasPrefix(part, ".") {
			return true
		}
	}
	return false
}

// dotFileHidingFile is the http.File use in dotFileHidingFileSystem.
// It is used to wrap the Readdir method of http.File so that we can
// remove files and directories that start with a period from its output.
type dotFileHidingFile struct {
	http.File
}

// Readdir is a wrapper around the Readdir method of the embedded File
// that filters out all files that start with a period in their name.
func (f dotFileHidingFile) Readdir(n int) (fis []fs.FileInfo, err error) {
	files, err := f.File.Readdir(n)
	for _, file := range files { // Filters out the dot files
		if !strings.HasPrefix(file.Name(), ".") {
			fis = append(fis, file)
		}
	}
	if err == nil && n > 0 && len(fis) == 0 {
		err = io.EOF
	}
	return
}

// dotFileHidingFileSystem is an http.FileSystem that hides
// hidden "dot files" from being served.
type dotFileHidingFileSystem struct {
	http.FileSystem
}

// Open is a wrapper around the Open method of the embedded FileSystem
// that serves a 403 permission error when name has a file or directory
// with whose name starts with a period in its path.
func (fsys dotFileHidingFileSystem) Open(name string) (http.File, error) {
	slog.Default().Debug("Trying to open", "name", name)
	if containsDotFile(name) { // If dot file, return 403 response
		return nil, fs.ErrPermission
	}

	file, err := fsys.FileSystem.Open(name)
	if err != nil {
		return nil, err
	}
	return dotFileHidingFile{file}, nil
}
