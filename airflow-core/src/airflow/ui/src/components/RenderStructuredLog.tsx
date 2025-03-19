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
import { chakra, Code } from "@chakra-ui/react";
import { Link } from "react-router-dom";

import type { StructuredLogMessage } from "openapi/requests/types.gen";
import Time from "src/components/Time";
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
        {logMessage}
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

  elements.push(
    <Link
      id={index.toString()}
      key={`line_${index}`}
      style={{
        display: "inline-block",
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
    </Link>,
  );

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

  const errorDetail = "error_detail" in structured ? structured.error_detail : undefined;
  let details;

  if (errorDetail !== undefined) {
    delete structured.error_detail;
    details = (errorDetail as Array<ErrorDetail>).map((error) => {
      const errorLines = error.frames.map((frame) => (
        <chakra.p key="test">
          File{" "}
          <chakra.span color="fg.info" key="test">
            &quot;{frame.filename}&quot;
          </chakra.span>
          , line {frame.lineno} in {frame.name}
        </chakra.p>
      ));

      return (
        <details key={error.exc_type} open={true} style={{ width: "100%" }}>
          <summary data-testid={`summary-${error.exc_type}`}>
            <chakra.span color="fg.info" cursor="pointer">
              <>
                {" "}
                {error.exc_type}: {error.exc_value}
              </>
            </chakra.span>
          </summary>
          {errorLines}
        </details>
      );
    });
  }

  elements.push(
    <chakra.span className="event" key={2} style={{ whiteSpace: "pre-wrap" }}>
      {event}:{details}
    </chakra.span>,
  );

  for (const key in structured) {
    if (Object.hasOwn(structured, key)) {
      elements.push(
        " ",
        <chakra.span color={key === "logger" ? "fg.info" : undefined} key={`prop_${key}`}>
          {key === "logger" ? "source" : key}={JSON.stringify(structured[key])}
        </chakra.span>,
      );
    }
  }

  return (
    <chakra.p key={index} lineHeight={1.5}>
      {elements}
    </chakra.p>
  );
};
