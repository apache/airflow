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

// sphinx-js extension config (loaded via the ``ts_sphinx_js_config`` Sphinx setting).
//
// The ts-sdk sources use TSDoc inline tags such as ``{@link TaskClient.getVariable}``.
// TypeDoc represents these as ``inline-tag`` comment parts, but sphinx-js's comment
// renderer only understands ``text`` and ``code`` parts and throws "Not implemented"
// on anything else. We register a TypeDoc resolve-end listener that flattens every
// unsupported comment part (``inline-tag``, ``relative-link``) into a ``code`` part
// wrapping the tag's own display text in backticks, so the referenced identifier renders
// as an inline literal, is skipped by sphinxcontrib-spelling, and never crashes the build.

import { Converter } from "typedoc";

/** Rewrite a single list of CommentDisplayParts in place. */
function flattenParts(parts) {
  if (!parts) {
    return;
  }
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part.kind !== "text" && part.kind !== "code") {
      parts[i] = { kind: "code", text: "`" + (part.text ?? "") + "`" };
    }
  }
}

/** Flatten every comment attached to a reflection (summary + block tags). */
function flattenComment(comment) {
  if (!comment) {
    return;
  }
  flattenParts(comment.summary);
  for (const tag of comment.blockTags ?? []) {
    flattenParts(tag.content);
  }
}

export const config = {
  async preConvert(app) {
    app.converter.on(Converter.EVENT_RESOLVE_END, (context) => {
      for (const reflection of Object.values(context.project.reflections)) {
        flattenComment(reflection.comment);
        for (const sig of reflection.signatures ?? []) {
          flattenComment(sig.comment);
        }
        flattenComment(reflection.getSignature?.comment);
        flattenComment(reflection.setSignature?.comment);
      }
    });
  },
};
