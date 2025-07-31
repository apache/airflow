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
import { parse as parseYaml } from "yaml";

export const detectLanguage = (value: string): string => {
  const trimmed = value.trim();

  // Try to detect JSON
  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    try {
      JSON.parse(trimmed);

      return "json";
    } catch {
      // Not valid JSON, continue
    }
  }

  // Try to detect YAML by parsing
  try {
    // Basic heuristics first - must contain colons or dashes for key-value pairs or lists
    if (trimmed.includes(":") || trimmed.includes("- ")) {
      parseYaml(trimmed);

      // If parsing succeeds and it's not just a simple string, it's likely YAML
      return "yaml";
    }
  } catch {
    // Not valid YAML, continue to other checks
  }

  // Try to detect SQL (basic heuristics)
  const sqlKeywords = /\b(?:select|insert|update|delete|create|alter|drop|from|where|join)\b/iu;

  if (sqlKeywords.test(trimmed)) {
    return "sql";
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

  // Default to text (no highlighting)
  return "text";
};
