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
import { Fragment } from "react";

import { Badge, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/system-components";

import { MonoText } from "./MonoText";

type PathTextProps = {
  readonly occurrences?: number;
  readonly path: string;
  // A truncated diff stopped counting early, so every count is a lower bound.
  readonly truncated?: boolean;
};

// A path has no spaces, and `overflow-wrap: anywhere` would let it break after any character,
// which drops the column's min-content width to one glyph — the other columns then squeeze it
// until the path reads vertically. Offering a break after each separator instead keeps the column
// at least as wide as the longest segment and breaks where the path already divides.
export const PathText = ({ occurrences = 1, path, truncated = false }: PathTextProps) => {
  const { t: translate } = useTranslation("dag");
  let walked = "";

  const text = (
    <MonoText overflowWrap="break-word">
      {path.split("/").map((segment, index) => {
        walked += index === 0 ? segment : `/${segment}`;

        return (
          <Fragment key={walked === "" ? "/" : walked}>
            {index === 0 ? undefined : "/"}
            <wbr />
            {segment}
          </Fragment>
        );
      })}
    </MonoText>
  );

  if (occurrences <= 1) {
    return text;
  }

  return (
    <HStack alignItems="baseline" gap={2}>
      {text}
      <Tooltip
        content={translate(truncated ? "versions.pathOccurrencesAtLeast" : "versions.pathOccurrences", {
          count: occurrences,
        })}
        portalled
      >
        <Badge colorPalette="purple" flexShrink={0} size="sm" variant="outline">
          {`×${occurrences}${truncated ? "+" : ""}`}
        </Badge>
      </Tooltip>
    </HStack>
  );
};
