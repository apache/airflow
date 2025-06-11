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
import { chakra, Code, Link } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { StructuredLogMessage } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { urlRegex } from "src/constants/urlRegex";
import { LogLevel, logLevelColorMapping } from "src/utils/logs";

type Frame = {
  filename: string;
  lineno: number;
  name: string;
};

type ErrorDetail = {
  exc_notes: Array<string>;
  exc_type: string;
  exc_value: string;
  frames: Array<Frame>;
  is_cause: boolean;
  syntax_error: string | null;
};

type RenderStructuredLogProps = {
  index: number;
  logLevelFilters?: Array<string>;
  logLink: string;
  logMessage: string | StructuredLogMessage;
  sourceFilters?: Array<string>;
};

const addLinks = (line: string) => {
  const matches = [...line.matchAll(urlRegex)];
  let currentIndex = 0;
  const elements: Array<JSX.Element | string> = [];

  if (!matches.length) {
    return line;
  }

  matches.forEach((match) => {
    const startIndex = match.index;

    // Add text before the URL
    if (startIndex > currentIndex) {
      elements.push(line.slice(currentIndex, startIndex));
    }

    elements.push(
      <Link
        color="fg.info"
        href={match[0]}
        key={match[0]}
        rel="noopener noreferrer"
        target="_blank"
        textDecoration="underline"
      >
        {match[0]}
      </Link>,
    );

    currentIndex = startIndex + match[0].length;
  });

  // Add remaining text after the last URL
  if (currentIndex < line.length) {
    elements.push(line.slice(currentIndex));
  }

  return elements;
};

export const renderStructuredLog = ({
  index,
  logLevelFilters,
  logLink,
  logMessage,
  sourceFilters,
}: RenderStructuredLogProps) => {
  if (typeof logMessage === "string") {
    return (
      <chakra.span key={index} lineHeight={1.5}>
        {addLinks(logMessage)}
      </chakra.span>
    );
  }

  const { event, level = undefined, timestamp, ...structured } = logMessage;

  const elements = [];

  if (
    logLevelFilters !== undefined &&
    Boolean(logLevelFilters.length) &&
    ((typeof level === "string" && !logLevelFilters.includes(level)) || !Boolean(level))
  ) {
    return "";
  }

  if (
    sourceFilters !== undefined &&
    Boolean(sourceFilters.length) &&
    (("logger" in structured && !sourceFilters.includes(structured.logger as string)) ||
      !("logger" in structured))
  ) {
    return "";
  }

  if (Boolean(timestamp)) {
    elements.push("[", <Time datetime={timestamp} key={0} />, "] ");
  }

  if (typeof level === "string") {
    elements.push(
      <Code
        colorPalette={level.toUpperCase() in LogLevel ? logLevelColorMapping[level as LogLevel] : undefined}
        key={1}
        lineHeight={1.5}
        minH={0}
        px={0}
      >
        {level.toUpperCase()}
      </Code>,
      " - ",
    );
  }

  const { error_detail: errorDetail, ...reStructured } = structured;
  let details;

  if (errorDetail !== undefined) {
    details = (errorDetail as Array<ErrorDetail>).map((error) => {
      const errorLines = error.frames.map((frame) => (
        <chakra.p key={`frame-${frame.name}-${frame.filename}-${frame.lineno}`}>
          File <chakra.span color="fg.info">{JSON.stringify(frame.filename)}</chakra.span>, line{" "}
          {frame.lineno} in {frame.name}
        </chakra.p>
      ));

      return (
        <chakra.details key={error.exc_type} ml="20em" open={true}>
          <chakra.summary data-testid={`summary-${error.exc_type}`}>
            <chakra.span color="fg.info" cursor="pointer">
              {error.exc_type}: {error.exc_value}
            </chakra.span>
          </chakra.summary>
          {errorLines}
        </chakra.details>
      );
    });
  }

  elements.push(
    <chakra.span className="event" key={2} whiteSpace="pre-wrap">
      {addLinks(event)}
    </chakra.span>,
  );

  for (const key in reStructured) {
    if (Object.hasOwn(reStructured, key)) {
      elements.push(
        ": ",
        <chakra.span color={key === "logger" ? "fg.info" : undefined} key={`prop_${key}`}>
          {key === "logger" ? "source" : key}={JSON.stringify(reStructured[key])}
        </chakra.span>,
      );
    }
  }

  elements.push(
    <chakra.span className="event" key={3} whiteSpace="pre-wrap">
      {details}
    </chakra.span>,
  );

  return (
    <chakra.div display="flex" key={index} lineHeight={1.5}>
      <RouterLink
        id={index.toString()}
        key={`line_${index}`}
        style={{
          display: "inline-block",
          flexShrink: 0,
          marginRight: "10px",
          paddingRight: "5px",
          textAlign: "right",
          userSelect: "none",
          WebkitUserSelect: "none",
          width: "3em",
        }}
        to={`${logLink}#${index}`}
      >
        {index}
      </RouterLink>
      <chakra.span overflow="auto" whiteSpace="pre-wrap" width="100%">
        {elements}
      </chakra.span>
    </chakra.div>
  );
};
