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
import { Skeleton, HStack, Text, Link } from "@chakra-ui/react";

import { useXcomServiceGetXcomEntry } from "openapi/queries";
import type { XComResponseNative } from "openapi/requests/types.gen";
import RenderedJsonField from "src/components/RenderedJsonField";
import { ClipboardIconButton, ClipboardRoot } from "src/components/ui";
import { urlRegex } from "src/constants/urlRegex";

type XComEntryProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly open?: boolean;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey: string;
};

const renderTextWithLinks = (text: string) => {
  const urls = text.match(urlRegex);
  const parts = text.split(/\s+/u);

  return (
    <>
      {parts.map((part, index) => {
        const isLastPart = index === parts.length - 1;

        if (urls?.includes(part)) {
          return (
            <Link
              color="fg.info"
              href={part}
              key={part}
              rel="noopener noreferrer"
              target="_blank"
              textDecoration="underline"
            >
              {part}
            </Link>
          );
        }

        return `${part}${isLastPart ? "" : " "}`;
      })}
    </>
  );
};

export const XComEntry = ({ dagId, mapIndex, open = false, runId, taskId, xcomKey }: XComEntryProps) => {
  const { data, isLoading } = useXcomServiceGetXcomEntry<XComResponseNative>({
    dagId,
    dagRunId: runId,
    deserialize: true,
    mapIndex,
    stringify: false,
    taskId,
    xcomKey,
  });
  // When deserialize=true, the API returns a stringified representation
  // so we don't need to JSON.stringify it again
  const xcomValue = data?.value;
  const isObjectOrArray = Array.isArray(xcomValue) || (xcomValue !== null && typeof xcomValue === "object");
  const valueFormatted = typeof xcomValue === "string" ? xcomValue : JSON.stringify(xcomValue, undefined, 4);

  return isLoading ? (
    <Skeleton
      data-testid="skeleton"
      display="inline-block"
      height="10px"
      width={200} // TODO: Make Skeleton take style from column definition
    />
  ) : (
    <HStack>
      {isObjectOrArray ? (
        <RenderedJsonField
          content={xcomValue as object}
          enableClipboard={false}
          jsonProps={{ collapsed: !open }}
        />
      ) : (
        <Text>{renderTextWithLinks(valueFormatted)}</Text>
      )}
      {xcomValue === undefined || xcomValue === null ? undefined : (
        <ClipboardRoot value={valueFormatted}>
          <ClipboardIconButton />
        </ClipboardRoot>
      )}
    </HStack>
  );
};
