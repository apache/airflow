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

import React from "react";
import { Code } from "@chakra-ui/react";
import type { CellContext } from "@tanstack/react-table";

import Time from "src/components/Time";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const TimeCell = ({ getValue }: CellContext<any, any>) => {
  const value = getValue();
  return <Time dateTime={value} />;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const CodeCell = ({ getValue }: CellContext<any, any>) => {
  const value = getValue();
  return value ? <Code>{value}</Code> : null;
};
