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

/* eslint-disable perfectionist/sort-enums */

/* eslint-disable perfectionist/sort-objects */
import { createListCollection } from "@chakra-ui/react";

export enum LogLevel {
  DEBUG = "debug",
  INFO = "info",
  WARNING = "warning",
  ERROR = "error",
  CRITICAL = "critical",
}

export const logLevelColorMapping = {
  [LogLevel.DEBUG]: "gray",
  [LogLevel.INFO]: "green",
  [LogLevel.WARNING]: "yellow",
  [LogLevel.ERROR]: "orange",
  [LogLevel.CRITICAL]: "red",
};

export const logLevelOptions = createListCollection<{
  label: string;
  value: string;
}>({
  items: [
    { label: "All Levels", value: "all" },
    { label: LogLevel.DEBUG.toUpperCase(), value: LogLevel.DEBUG },
    { label: LogLevel.INFO.toUpperCase(), value: LogLevel.INFO },
    { label: LogLevel.WARNING.toUpperCase(), value: LogLevel.WARNING },
    { label: LogLevel.ERROR.toUpperCase(), value: LogLevel.ERROR },
    { label: LogLevel.CRITICAL.toUpperCase(), value: LogLevel.CRITICAL },
  ],
});
