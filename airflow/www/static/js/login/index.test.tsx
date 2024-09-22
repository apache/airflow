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

/* global describe  */

import React from "react";

import { render } from "@testing-library/react";

import { Wrapper } from "src/utils/testUtils";
import Login from ".";

describe("Login page", () => {
  test("Components renders properly", () => {
    const { getAllByText } = render(<Login />, {
      wrapper: Wrapper,
    });

    expect(getAllByText("Sign in")).toHaveLength(2);
    expect(getAllByText("Enter your login and password below:")).toHaveLength(
      1
    );
    expect(getAllByText("Username")).toHaveLength(1);
    expect(getAllByText("Password")).toHaveLength(1);
  });
});
