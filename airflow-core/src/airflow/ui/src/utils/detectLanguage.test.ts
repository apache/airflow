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

/**
 * @vitest-environment node
 */
import { init } from "@guanmingchiu/sqlparser-ts";
import { beforeAll, describe, expect, it } from "vitest";

import { detectLanguage } from "./detectLanguage";

beforeAll(async () => {
  await init();
});

describe("detectLanguage", () => {
  describe("JSON detection", () => {
    it("detects valid JSON object", () => {
      expect(detectLanguage('{"key": "value"}')).toBe("json");
    });

    it("detects valid JSON array", () => {
      expect(detectLanguage("[1, 2, 3]")).toBe("json");
    });

    it("detects nested JSON", () => {
      expect(detectLanguage('{"nested": {"key": "value"}}')).toBe("json");
    });
  });

  describe("SQL detection", () => {
    it("detects SELECT statement", () => {
      expect(detectLanguage("SELECT * FROM users")).toBe("sql");
    });

    it("detects SELECT with WHERE clause", () => {
      expect(detectLanguage("SELECT id, name FROM users WHERE id = 1")).toBe("sql");
    });

    it("detects INSERT statement", () => {
      expect(detectLanguage("INSERT INTO users (name) VALUES ('test')")).toBe("sql");
    });

    it("detects UPDATE statement", () => {
      expect(detectLanguage("UPDATE users SET name = 'test' WHERE id = 1")).toBe("sql");
    });

    it("detects DELETE statement", () => {
      expect(detectLanguage("DELETE FROM users WHERE id = 1")).toBe("sql");
    });

    it("detects CREATE TABLE statement", () => {
      expect(detectLanguage("CREATE TABLE users (id INT, name VARCHAR(255))")).toBe("sql");
    });

    it("detects WITH (CTE) statement", () => {
      expect(detectLanguage("WITH cte AS (SELECT 1) SELECT * FROM cte")).toBe("sql");
    });

    it("detects multiline SQL", () => {
      const sql = `
        SELECT *
        FROM users
        WHERE id = 1
      `;

      expect(detectLanguage(sql)).toBe("sql");
    });
  });

  describe("Bash detection", () => {
    it("detects shebang", () => {
      expect(detectLanguage("#!/bin/bash\necho hello")).toBe("bash");
    });

    it("detects common bash commands", () => {
      expect(detectLanguage("echo 'Hello World'")).toBe("bash");
      expect(detectLanguage("ls -la")).toBe("bash");
      expect(detectLanguage("cd /tmp")).toBe("bash");
    });

    it("detects pipe operator", () => {
      expect(detectLanguage("cat file.txt | grep pattern")).toBe("bash");
    });

    it("detects command substitution", () => {
      expect(detectLanguage("echo $(date)")).toBe("bash");
    });

    it("detects logical operators", () => {
      expect(detectLanguage("command1 && command2")).toBe("bash");
      expect(detectLanguage("command1 || command2")).toBe("bash");
    });
  });

  describe("YAML detection", () => {
    it("detects simple YAML", () => {
      expect(detectLanguage("key: value")).toBe("yaml");
    });

    it("detects nested YAML", () => {
      const yaml = `
parent:
  child: value
`;

      expect(detectLanguage(yaml)).toBe("yaml");
    });

    it("detects YAML list", () => {
      const yaml = `
items:
  - item1
  - item2
`;

      expect(detectLanguage(yaml)).toBe("yaml");
    });
  });

  describe("edge cases", () => {
    it("handles string with leading/trailing whitespace", () => {
      expect(detectLanguage("  SELECT * FROM users  ")).toBe("sql");
    });

    it("prioritizes JSON over YAML for valid JSON", () => {
      // Valid JSON is also valid YAML, but JSON should be detected first
      expect(detectLanguage('{"key": "value"}')).toBe("json");
    });
  });
});
