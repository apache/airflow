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
import { Parser } from "node-sql-parser";
import { parse as parseYaml } from "yaml";

export const detectLanguage = (value: string): string => {
  const trimmed = value.trim();

  // Try to detect JSON
  try {
    JSON.parse(trimmed);

    return "json";
  } catch {
    // Not valid JSON, continue
  }

  // Try to detect SQL by parsing with node-sql-parser
  try {
    const parser = new Parser();

    // Support multiple SQL dialects
    parser.astify(trimmed, { database: "postgresql" });

    return "sql";
  } catch {
    // Try with other dialects if PostgreSQL fails
    try {
      const parser = new Parser();

      parser.astify(trimmed, { database: "mysql" });

      return "sql";
    } catch {
      // Not valid SQL, continue to other checks
    }
  }

  // Try to detect Bash (basic heuristics)
  const bashKeywords =
    /\b(?:echo|ls|cd|mkdir|rm|cp|mv|grep|awk|sed|cat|chmod|chown|ps|kill|sudo|export|source|if|then|else|fi|for|while|do|done)\b/u;
  const bashCommands = /\$\{|\$\(|&&|\|\||>>|<<|;/u;
  const bashPipe = /(?:^|[^|])\|(?:[^|]|$)/u;

  if (
    trimmed.startsWith("#!") ||
    bashKeywords.test(trimmed) ||
    bashCommands.test(trimmed) ||
    bashPipe.test(trimmed)
  ) {
    return "bash";
  }

  // Try to detect YAML by parsing - check last since it's very permissive
  try {
    parseYaml(trimmed);

    // If parsing succeeds and it's not just a simple string, it's likely YAML
    return "yaml";
  } catch {
    // Not valid YAML, continue to other checks
  }

  // Default to text (no highlighting)
  return "text";
};
