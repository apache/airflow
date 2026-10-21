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
import RenderedJsonField from "src/components/RenderedJsonField";

import { MonoText } from "./MonoText";

// A scalar can still be long — a templated command, a doc string — and nothing below it is worth
// the vertical space of a whole cell.
const MAX_VALUE_LENGTH = 120;

// This table is the one place with two JSON previews per row, and the component's own cap leaves
// them demanding more width than the pane has. The rest of a value is one expand away.
const PREVIEW_MAX_WIDTH = 220;

type DiffValueProps = {
  readonly value: unknown;
};

export const DiffValue = ({ value }: DiffValueProps) => {
  // A side the change does not have is left blank: a dash would read as a value the version holds.
  if (value === undefined) {
    return undefined;
  }

  // A whole task added or removed carries its entire serialized payload. The JSON field names its
  // top-level keys in badges and opens the editor in place, where a flattened string could only be
  // cut short. An empty object or array has no entries to preview, so it falls through to the text
  // below and shows as `{}` or `[]`.
  if (typeof value === "object" && value !== null && Object.keys(value).length > 0) {
    return <RenderedJsonField collapsed content={value} previewMaxWidth={PREVIEW_MAX_WIDTH} />;
  }

  // A stored null is a value; only an absent side gets the blank cell above. An empty string is
  // quoted so it reads as a value rather than as a blank cell.
  const text = typeof value === "string" ? value || '""' : JSON.stringify(value);

  // Serialized values have no spaces to break at, so without this a single cell widens the table
  // past the pane and the last ("after") column is clipped.
  return (
    <MonoText overflowWrap="anywhere">
      {text.length > MAX_VALUE_LENGTH ? `${text.slice(0, MAX_VALUE_LENGTH)}…` : text}
    </MonoText>
  );
};
