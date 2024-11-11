// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// wordcount is an example that counts words in Shakespeare and demonstrates
// Beam best practices.
//
// This example is the second in a series of four successively more detailed
// 'word count' examples. You may first want to take a look at
// minimal_wordcount. After you've looked at this example, see the
// debugging_wordcount pipeline for introduction of additional concepts.
//
// For a detailed walkthrough of this example, see
//
//    https://beam.apache.org/get-started/wordcount-example/
//
// Basic concepts, also in the minimal_wordcount example: reading text files;
// counting a PCollection; writing to text files.
//
// New concepts:
//
//  1. Executing a pipeline both locally and using the selected runner
//  2. Defining your own pipeline options
//  3. Using ParDo with static DoFns defined out-of-line
//  4. Building a composite transform
//
// Concept #1: You can execute this pipeline either locally or by
// selecting another runner. These are now command-line options added by
// the 'beamx' package and not hard-coded as they were in the minimal_wordcount
// example. The 'beamx' package also registers all included runners and
// filesystems as a convenience.
//
// To change the runner, specify:
//
//    --runner=YOUR_SELECTED_RUNNER
//
// To execute this pipeline, specify a local output file (if using the
// 'direct' runner) or a remote file on a supported distributed file system.
//
//    --output=[YOUR_LOCAL_FILE | YOUR_REMOTE_FILE]
//
// The input file defaults to a public data set containing the text of King
// Lear by William Shakespeare. You can override it and choose your own input
// with --input.
package main

// beam-playground:
//   name: WordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options: --output output.txt
//   context_line: 120
//   categories:
//     - Combiners
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - count
//     - io
//     - strings

import (
    "context"
    "flag"
    "fmt"
    "log"
    "regexp"
    "strings"

    "github.com/apache/beam/sdks/v2/go/pkg/beam"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/register"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

// Concept #2: Defining your own configuration options. Pipeline options can
// be standard Go flags, or they can be obtained any other way. Defining and
// configuring the pipeline is normal Go code.
var (
    // By default, this example reads from a public dataset containing the text of
    // King Lear. Set this option to choose a different input file or glob.
    input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")

    // Set this required option to specify where to write the output.
    output = flag.String("output", "", "Output file (required).")
)

// Concept #3: You can make your pipeline assembly code less verbose by
// defining your DoFns statically out-of-line. A DoFn can be defined as a Go
// function and is conventionally suffixed "Fn". Using named function
// transforms allows for easy reuse, modular testing, and an improved monitoring
// experience. The argument and return types of a function dictate the pipeline
// shape when used in a ParDo. For example,
//
//    func formatFn(w string, c int) string
//
// indicates that the function operates on a PCollection of type KV<string,int>,
// representing key value pairs of strings and ints, and outputs a PCollection
// of type string. Beam typechecks the pipeline before running it.
//
// DoFns that potentially output zero or multiple elements can also be Go
// functions, but have a different signature. For example,
//
//    func extractFn(w string, emit func(string))
//
// uses an "emit" function argument instead of a string return type to allow it
// to output any number of elements. It operates on a PCollection of type string
// and returns a PCollection of type string.
//
// DoFns must be registered with Beam in order to be executed in ParDos. This is
// done automatically by the starcgen code generator, or it can be done manually
// by calling beam.RegisterFunction in an init() call.
func init() {
    // register.DoFnXxY registers a struct DoFn so that it can be correctly
    // serialized and does some optimization to avoid runtime reflection. Since
    // extractFn has 3 inputs and 0 outputs, we use register.DoFn3x0 and provide
    // its input types as its constraints (if it had any outputs, we would add
    // those as constraints as well). Struct DoFns must be registered for a
    // pipeline to run.
    register.DoFn3x0[context.Context, string, func(string)](&extractFn{})
    // register.FunctionXxY registers a functional DoFn to optimize execution at
    // runtime. formatFn has 2 inputs and 1 output, so we use
    // register.Function2x1.
    register.Function2x1(formatFn)
    // register.EmitterX is optional and will provide some optimization to make
    // things run faster. Any emitters (functions that produce output for the next
    // step) should be registered. Here we register all emitters with the
    // signature func(string).
    register.Emitter1[string]()
}

var (
    wordRE          = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
    empty           = beam.NewCounter("extract", "emptyLines")
    smallWordLength = flag.Int("small_word_length", 9, "length of small words (default: 9)")
    smallWords      = beam.NewCounter("extract", "smallWords")
    lineLen         = beam.NewDistribution("extract", "lineLenDistro")
)

// extractFn is a structural DoFn that emits the words in a given line and keeps
// a count for small words. Its ProcessElement function will be invoked on each
// element in the input PCollection.
type extractFn struct {
    SmallWordLength int `json:"smallWordLength"`
}

func (f *extractFn) ProcessElement(ctx context.Context, line string, emit func(string)) {
    lineLen.Update(ctx, int64(len(line)))
    if len(strings.TrimSpace(line)) == 0 {
        empty.Inc(ctx, 1)
    }
    for _, word := range wordRE.FindAllString(line, -1) {
        // increment the counter for small words if length of words is
        // less than small_word_length
        if len(word) < f.SmallWordLength {
            smallWords.Inc(ctx, 1)
        }
        emit(word)
    }
}

// formatFn is a functional DoFn that formats a word and its count as a string.
func formatFn(w string, c int) string {
    return fmt.Sprintf("%s: %v", w, c)
}

// Concept #4: A composite PTransform is a Go function that adds
// transformations to a given pipeline. It is run at construction time and
// works on PCollections as values. For monitoring purposes, the pipeline
// allows scoped naming for composite transforms. The difference between a
// composite transform and a construction helper function is solely in whether
// a scoped name is used.
//
// For example, the CountWords function is a custom composite transform that
// bundles two transforms (ParDo and Count) as a reusable function.

// CountWords is a composite transform that counts the words of a PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>. The Beam type checker enforces these constraints
// during pipeline construction.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
    s = s.Scope("CountWords")

    // Convert lines of text into individual words.
    col := beam.ParDo(s, &extractFn{SmallWordLength: *smallWordLength}, lines)

    // Count the number of times each word occurs.
    return stats.Count(s, col)
}

func main() {
    // If beamx or Go flags are used, flags must be parsed first.
    flag.Parse()
    // beam.Init() is an initialization hook that must be called on startup. On
    // distributed runners, it is used to intercept control.
    beam.Init()

    // Input validation is done as usual. Note that it must be after Init().
    if *output == "" {
        log.Fatal("No output provided")
    }

    // Concepts #3 and #4: The pipeline uses the named transform and DoFn.
    p := beam.NewPipeline()
    s := p.Root()

    lines := textio.Read(s, *input)
    counted := CountWords(s, lines)
    formatted := beam.ParDo(s, formatFn, counted)
    textio.Write(s, *output, formatted)

    // Concept #1: The beamx.Run convenience wrapper allows a number of
    // pre-defined runners to be used via the --runner flag.
    if err := beamx.Run(context.Background(), p); err != nil {
        log.Fatalf("Failed to execute job: %v", err)
    }
}
