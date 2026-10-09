/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, it } from "vitest";
import * as hitlModule from "../../src/hitl/index.js";
import { approval, hitl, type HITLSpec } from "../../src/hitl/index.js";
import { withArgNames } from "../../src/sdk/arg-names.js";

describe("the hitl subpath", () => {
  it("exports the two factories and nothing else at run time", () => {
    expect(Object.keys(hitlModule).sort()).toEqual(["approval", "hitl"]);
  });
});

describe("hitl", () => {
  it("applies the operator's defaults", () => {
    const task = hitl({ subject: "Pick", options: ["a", "b"] });

    expect(task).toMatchObject({
      kind: "choice",
      subject: "Pick",
      body: undefined,
      options: ["a", "b"],
      defaults: undefined,
      multiple: false,
      assignedUsers: [],
      params: {},
      responseTimeout: undefined,
    });
    expect(Object.isFrozen(task)).toBe(true);
  });

  it("copies the options, so editing the array passed in changes nothing", () => {
    const options = ["a", "b"];
    const task = hitl({ subject: "Pick", options });
    options.push("c");

    expect(task.options).toEqual(["a", "b"]);
  });

  it("allows several defaults when several options may be chosen", () => {
    const task = hitl({
      subject: "s",
      options: ["a", "b"],
      defaults: ["a", "b"],
      multiple: true,
    });

    expect(task.defaults).toEqual(["a", "b"]);
  });

  it("takes the users allowed to respond", () => {
    const task = hitl({
      subject: "s",
      options: ["a"],
      assignedUsers: [{ id: "ada", name: "Ada" }],
    });

    expect(task.assignedUsers).toEqual([{ id: "ada", name: "Ada" }]);
  });

  describe("params", () => {
    it("keeps each field as a frozen copy, so editing what was passed in changes nothing", () => {
      const params = {
        region: {
          value: "us",
          description: "Where to ship",
          schema: { type: "string", enum: ["us", "eu"] },
        },
        retries: { value: 3 },
      };
      const task = hitl({ subject: "s", options: ["a"], params });
      params.region.schema.enum.push("apac");

      expect(task.params).toEqual({
        region: {
          value: "us",
          description: "Where to ship",
          schema: { type: "string", enum: ["us", "eu"] },
        },
        retries: { value: 3 },
      });
      expect(Object.isFrozen(task.params)).toBe(true);
      expect(Object.isFrozen(task.params["region"])).toBe(true);
    });

    it("allows a null value", () => {
      expect(
        hitl({ subject: "s", options: ["a"], params: { note: { value: null } } }).params,
      ).toEqual({ note: { value: null } });
    });
  });

  describe("merges the withArgNames renames of the subject and the body", () => {
    const subject = withArgNames(
      { version: "release_version" },
      ({ version }: { version: string }) => version,
    );
    const body = withArgNames({ notes: "release_notes" }, ({ notes }: { notes: string }) => notes);

    it("when both are functions", () => {
      const task = hitl({ subject, body, options: ["a"] });

      expect(task.argNames).toEqual(
        new Map([
          ["version", "release_version"],
          ["notes", "release_notes"],
        ]),
      );
    });

    it("when only one is a function", () => {
      expect(hitl({ subject, body: "fixed", options: ["a"] }).argNames).toEqual(
        new Map([["version", "release_version"]]),
      );
      expect(hitl({ subject: "fixed", body, options: ["a"] }).argNames).toEqual(
        new Map([["notes", "release_notes"]]),
      );
    });

    it("when both rename the same input the same way", () => {
      const sameRename = withArgNames(
        { version: "release_version" },
        ({ version }: { version: string }) => `Notes for ${version}`,
      );

      expect(hitl({ subject, body: sameRename, options: ["a"] }).argNames).toEqual(
        new Map([["version", "release_version"]]),
      );
    });

    it("and rejects two names for one input", () => {
      const elsewhere = withArgNames(
        { version: "tag" },
        ({ version }: { version: string }) => `Notes for ${version}`,
      );

      expect(() => hitl({ subject, body: elsewhere, options: ["a"] })).toThrowError(
        'hitl(...) maps the input "version" to "release_version" in its subject and to "tag" in its body; use one name for it',
      );
    });
  });

  describe("rejects", () => {
    it.each<[string, unknown, RegExp]>([
      [
        "no options",
        { subject: "s", options: [] },
        /hitl\(\.\.\.\) needs "options": a non-empty array of strings/,
      ],
      [
        "a duplicate option",
        { subject: "s", options: ["a", "a"] },
        /option "options" lists "a" twice/,
      ],
      [
        "an empty option",
        { subject: "s", options: [""] },
        /option "options" holds a value that is not a non-empty string/,
      ],
      [
        "a default it does not offer",
        { subject: "s", options: ["a"], defaults: ["b"] },
        /option "defaults" holds "b", which is not one of the options \["a"\]/,
      ],
      [
        "two defaults for a single choice",
        { subject: "s", options: ["a", "b"], defaults: ["a", "b"] },
        /hitl\(\.\.\.\) gives 2 defaults, but "multiple" is not set/,
      ],
      [
        "no subject",
        { options: ["a"] },
        /hitl\(\.\.\.\) needs a "subject": a string, or a function returning one/,
      ],
      ["an empty subject", { subject: "", options: ["a"] }, /option "subject" cannot be empty/],
      [
        "a numeric body",
        { subject: "s", body: 3, options: ["a"] },
        /option "body" must be a string, or a function returning one/,
      ],
      [
        "a zero timeout",
        { subject: "s", options: ["a"], responseTimeout: 0 },
        /"responseTimeout" must be a positive whole number of seconds/,
      ],
      [
        "a fractional timeout",
        { subject: "s", options: ["a"], responseTimeout: 1.5 },
        /"responseTimeout" must be a positive whole number of seconds/,
      ],
      [
        "an assigned user with no id",
        { subject: "s", options: ["a"], assignedUsers: [{ id: "", name: "Ada" }] },
        /option "assignedUsers" holds \{"id":"","name":"Ada"\}; each user is \{ id, name \}/,
      ],
      [
        "an option of the approval",
        { subject: "s", options: ["a"], failOnReject: true },
        /Unknown option "failOnReject" for hitl\(\.\.\.\)/,
      ],
      [
        "the old name of assignedUsers",
        { subject: "s", options: ["a"], assignees: [] },
        /Unknown option "assignees" for hitl\(\.\.\.\)/,
      ],
      [
        "params that are not an object",
        { subject: "s", options: ["a"], params: [] },
        /option "params" must be an object of \{ value, \.\.\. \} by name/,
      ],
      [
        "a param named _options",
        { subject: "s", options: ["a"], params: { _options: { value: "x" } } },
        /option "params": "_options" is not allowed in params/,
      ],
      [
        "a param that is not an object",
        { subject: "s", options: ["a"], params: { region: "us" } },
        /param "region" must be an object of \{ value, \.\.\. \}/,
      ],
      [
        "a param with no value",
        { subject: "s", options: ["a"], params: { region: { description: "Where" } } },
        /param "region" needs a "value"/,
      ],
      [
        "a param value that is not JSON",
        { subject: "s", options: ["a"], params: { when: { value: new Date(0) } } },
        /param "when" key "value" holds a Date, which JSON cannot carry/,
      ],
      [
        "a param schema that is not JSON",
        { subject: "s", options: ["a"], params: { n: { value: 1, schema: { max: Infinity } } } },
        /param "n" key "schema" holds Infinity, which JSON cannot carry/,
      ],
      [
        "a param with a key it does not declare",
        { subject: "s", options: ["a"], params: { region: { value: "us", default: "us" } } },
        /param "region" has an unknown key "default"/,
      ],
      [
        "a param description that is not a string",
        { subject: "s", options: ["a"], params: { region: { value: "us", description: 1 } } },
        /param "region" key "description" must be a string/,
      ],
      [
        "a param schema that is not an object",
        { subject: "s", options: ["a"], params: { region: { value: "us", schema: "string" } } },
        /param "region" key "schema" must be a JSON Schema object/,
      ],
    ])("%s", (_label, spec, error) => {
      expect(() => hitl(spec as HITLSpec)).toThrowError(error);
    });
  });
});

describe("approval", () => {
  it("offers exactly Approve and Reject, and skips what directly follows on Reject by default", () => {
    expect(approval({ subject: "Ship?" })).toMatchObject({
      kind: "approval",
      options: ["Approve", "Reject"],
      multiple: false,
      ignoreDownstreamTriggerRules: false,
      failOnReject: false,
    });
  });

  it("takes the two Reject options and the shared ones", () => {
    const task = approval({
      subject: "Ship?",
      ignoreDownstreamTriggerRules: true,
      failOnReject: true,
      assignedUsers: [{ id: "ada", name: "Ada" }],
      params: { reason: { value: "" } },
    });

    expect(task).toMatchObject({
      ignoreDownstreamTriggerRules: true,
      failOnReject: true,
      assignedUsers: [{ id: "ada", name: "Ada" }],
      params: { reason: { value: "" } },
    });
  });

  it("takes one default, the answer given on timeout", () => {
    expect(approval({ subject: "Ship?", defaults: "Reject" }).defaults).toEqual(["Reject"]);
  });

  describe("rejects", () => {
    it.each<[string, unknown, RegExp]>([
      [
        "options",
        { subject: "s", options: ["a"] },
        /Unknown option "options" for approval\(\.\.\.\)/,
      ],
      [
        "multiple",
        { subject: "s", multiple: true },
        /Unknown option "multiple" for approval\(\.\.\.\)/,
      ],
      [
        "a default it does not offer",
        { subject: "s", defaults: "Maybe" },
        /option "defaults" must be "Approve" or "Reject"/,
      ],
      [
        "the old onReject",
        { subject: "s", onReject: "fail" },
        /Unknown option "onReject" for approval\(\.\.\.\)/,
      ],
      [
        "a failOnReject that is not a boolean",
        { subject: "s", failOnReject: "yes" },
        /option "failOnReject" must be a boolean/,
      ],
      [
        "an ignoreDownstreamTriggerRules that is not a boolean",
        { subject: "s", ignoreDownstreamTriggerRules: 1 },
        /option "ignoreDownstreamTriggerRules" must be a boolean/,
      ],
      [
        "a param named _options",
        { subject: "s", params: { _options: { value: "x" } } },
        /approval\(\.\.\.\) option "params": "_options" is not allowed in params/,
      ],
    ])("%s", (_label, spec, error) => {
      expect(() => approval(spec as never)).toThrowError(error);
    });
  });
});
