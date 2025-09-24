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
import { HStack, Skeleton, Text, Link } from "@chakra-ui/react";

import { useXcomServiceGetXcomEntry } from "openapi/queries";
import type { XComResponseNative } from "openapi/requests/types.gen";
import RenderedJsonField from "src/components/RenderedJsonField";

export type XComEntryProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly open?: boolean;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey: string;
};

const urlRegex = /(?:https?:\/\/|www\.)[^\s<>()]+(?:\([\w\d\-._~:/?#[\]@!$&'()*+,;=%]*\)|[^\s<>()])/giu;

const renderTextWithLinks = (text: string) => {
  if (!text) {
    return undefined;
  }
  const parts = text.split(/\s+/u);
  const urls: Array<string> = text.match(urlRegex) ?? [];
  const seen = new Map<string, number>();

  return (
    <>
      {parts.map((part) => {
        const count = (seen.get(part) ?? 0) + 1;

        seen.set(part, count);
        const key = `${part}-${count}`;
        const isUrl = urls.includes(part);

        if (isUrl) {
          const href = part.startsWith("http") ? part : `https://${part}`;

          return (
            <Link
              color="fg.info"
              href={href}
              key={key}
              rel="noopener noreferrer"
              target="_blank"
              textDecoration="underline"
            >
              {part}
            </Link>
          );
        }

        return <span key={key}>{part} </span>;
      })}
    </>
  );
};

// Type guard without short identifier and without using null literals
const isObjectLike = (valueCandidate: unknown): valueCandidate is object =>
  Boolean(valueCandidate) && typeof valueCandidate === "object";

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

  const value = data?.value;

  const valueFormatted = value === undefined ? "" : typeof value === "string" ? value : JSON.stringify(value);

  if (isLoading) {
    return <Skeleton data-testid="skeleton" display="inline-block" height="10px" width={200} />;
  }

  return (
    <HStack>
      {isObjectLike(value) ? (
        <RenderedJsonField content={value} jsonProps={{ collapsed: !open }} />
      ) : (
        <Text>{renderTextWithLinks(valueFormatted)}</Text>
      )}
    </HStack>
  );
};
