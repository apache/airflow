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
import type { TFunction } from "i18next";
import * as React from "react";
import { Link as RouterLink } from "react-router-dom";

import type { StructuredLogMessage } from "openapi/requests/types.gen";
import AnsiRenderer from "src/components/AnsiRenderer";
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
  renderingMode?: "jsx" | "text";
  showSource?: boolean;
  showTimestamp?: boolean;
  sourceFilters?: Array<string>;
  translate: TFunction;
};

const addAnsiWithLinks = (line: string) => {
  const urlMatches = [...line.matchAll(urlRegex)];

  if (!urlMatches.length) {
    return <AnsiRenderer linkify={false}>{line}</AnsiRenderer>;
  }

  let currentIndex = 0;
  const elements: Array<React.ReactNode> = [];

  urlMatches.forEach((match) => {
    const { index: startIndex } = match;

    if (startIndex > currentIndex) {
      const textBeforeUrl = line.slice(currentIndex, startIndex);

      elements.push(
        <AnsiRenderer key={`ansi-before-${textBeforeUrl}`} linkify={false}>
          {textBeforeUrl}
        </AnsiRenderer>,
      );
    }

    elements.push(
      <Link
        color="fg.info"
        href={match[0]}
        key={`link-${match[0]}-${startIndex}`}
        rel="noopener noreferrer"
        target="_blank"
        textDecoration="underline"
      >
        {match[0]}
      </Link>,
    );

    currentIndex = startIndex + match[0].length;
  });

  if (currentIndex < line.length) {
    const textAfterUrl = line.slice(currentIndex);

    elements.push(
      <AnsiRenderer key="ansi-after" linkify={false}>
        {textAfterUrl}
      </AnsiRenderer>,
    );
  }

  return elements;
};

const sourceFields = ["logger", "chan", "lineno", "filename", "loc"];

const renderStructuredLogImpl = ({
  index,
  logLevelFilters,
  logLink,
  logMessage,
  renderingMode = "jsx",
  showSource = true,
  showTimestamp = true,
  sourceFilters,
  translate,
}: RenderStructuredLogProps): JSX.Element | string => {
  if (typeof logMessage === "string") {
    if (renderingMode === "text") {
      return logMessage;
    }

    return (
      <chakra.span key={index} lineHeight={1.5}>
        {addAnsiWithLinks(logMessage)}
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

  if (Boolean(timestamp) && showTimestamp) {
    if (renderingMode === "text") {
      elements.push(`[${timestamp}] `);
    } else {
      elements.push("[", <Time datetime={timestamp} key={0} />, "] ");
    }
  }

  if (typeof level === "string") {
    const formattedLevel = level.toUpperCase();

    if (renderingMode === "text") {
      elements.push(`${formattedLevel} - `);
    } else {
      elements.push(
        <Code
          colorPalette={level.toUpperCase() in LogLevel ? logLevelColorMapping[level as LogLevel] : undefined}
          key={1}
          lineHeight={1.5}
          minH={0}
          px={0}
        >
          {formattedLevel}
        </Code>,
        " - ",
      );
    }
  }

  const { error_detail: errorDetail, ...reStructured } = structured;
  let details;

  if (errorDetail !== undefined) {
    details = (errorDetail as Array<ErrorDetail>).map((error) => {
      const errorLines = error.frames.map((frame) => {
        if (renderingMode === "text") {
          return `    ${translate("components:logs.file")} ${frame.filename}, ${translate("components:logs.location", { line: frame.lineno, name: frame.name })}\n`;
        }

        return (
          <chakra.p key={`frame-${frame.name}-${frame.filename}-${frame.lineno}`}>
            {translate("components:logs.file")}{" "}
            <chakra.span color="fg.info">{JSON.stringify(frame.filename)}</chakra.span>,{" "}
            {translate("components:logs.location", { line: frame.lineno, name: frame.name })}
          </chakra.p>
        );
      });

      if (renderingMode === "text") {
        return `${error.exc_type}: ${error.exc_value}\n${(errorLines as Array<string>).join("")}`;
      }

      return (
        <chakra.details key={error.exc_type} ms="20em" open={true}>
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
    renderingMode === "text" ? (
      event
    ) : (
      <chakra.span className="event" key={2} whiteSpace="pre-wrap">
        {addAnsiWithLinks(event)}
      </chakra.span>
    ),
  );

  if (Object.hasOwn(reStructured, "filename") && Object.hasOwn(reStructured, "lineno")) {
    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
    reStructured.loc = `${reStructured.filename}:${reStructured.lineno}`;
    delete reStructured.filename;
    delete reStructured.lineno;
  }

  for (const key in reStructured) {
    if (Object.hasOwn(reStructured, key)) {
      if (!showSource && sourceFields.includes(key)) {
        continue; // eslint-disable-line no-continue
      }
      const val = reStructured[key] as boolean | number | object | string | null;

      // Let strings, ints, etc through as is, but JSON stringify anything more complex
      const stringifiedValue = val instanceof Object ? JSON.stringify(val) : val;

      if (renderingMode === "text") {
        elements.push(`${key === "logger" ? "source" : key}=${stringifiedValue} `);
      } else {
        elements.push(
          <React.Fragment key={`space_${key}`}> </React.Fragment>,
          <span data-key={key} key={`struct_${key}`}>
            <chakra.span color="fg.info">{key === "logger" ? "source" : key}</chakra.span>=
            <span data-value>{stringifiedValue}</span>
          </span>,
        );
      }
    }
  }

  elements.push(
    renderingMode === "text" ? (
      details
    ) : (
      <chakra.span className="event" key={3} whiteSpace="pre-wrap">
        {details}
      </chakra.span>
    ),
  );

  if (renderingMode === "text") {
    return (elements as Array<string>).join("");
  }

  return (
    <chakra.div display="flex" key={index} lineHeight={1.5}>
      <RouterLink
        id={index.toString()}
        key={`line_${index}`}
        style={{
          display: "inline-block",
          flexShrink: 0,
          marginInlineEnd: "10px",
          paddingInlineEnd: "5px",
          textAlign: "end",
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

// Overloads for renderStructuredLog function for stick type safety
type RenderStructuredLogOverloads = {
  (props: { renderingMode: "jsx" } & Omit<RenderStructuredLogProps, "renderingMode">): JSX.Element | "";
  (props: { renderingMode: "text" } & Omit<RenderStructuredLogProps, "renderingMode">): string;
};

export const renderStructuredLog: RenderStructuredLogOverloads =
  renderStructuredLogImpl as unknown as RenderStructuredLogOverloads;
