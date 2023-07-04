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

/* global describe, test, expect */

import React from "react";
import { render } from "@testing-library/react";

import { ChakraWrapper } from "src/utils/testUtils";

import TaskName from "./TaskName";

describe("Test TaskName", () => {
  test("Displays a normal task name", () => {
    const { getByText } = render(
      <TaskName label="test" onToggle={() => {}} />,
      { wrapper: ChakraWrapper }
    );

    expect(getByText("test")).toBeDefined();
  });

  test("Displays a mapped task name", () => {
    const { getByText } = render(
      <TaskName level={0} label="test" isMapped onToggle={() => {}} />,
      { wrapper: ChakraWrapper }
    );

    expect(getByText("test [ ]")).toBeDefined();
  });

  test("Displays a group task name", () => {
    const { getByText, getByTestId } = render(
      <TaskName level={0} label="test" isGroup onToggle={() => {}} />,
      { wrapper: ChakraWrapper }
    );

    expect(getByText("test")).toBeDefined();
    expect(getByTestId("open-group")).toBeDefined();
  });
});
