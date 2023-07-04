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

import type { AxiosError } from "axios";
import { getErrorDescription } from "./useErrorToast";

describe("Test getErrorDescription()", () => {
  test("Returns expected results", () => {
    let description;

    // @ts-ignore
    const axiosError: AxiosError = new Error("Error message");

    axiosError.toJSON = () => ({});
    axiosError.response = {
      data: "Not available for this Executor",
      status: 400,
      statusText: "BadRequest",
      headers: {},
      config: {},
    };
    axiosError.isAxiosError = true;

    // if response.data is defined
    description = getErrorDescription(axiosError);
    expect(description).toBe("Not available for this Executor");

    axiosError.response.data = "";

    // if it is not, use the error message
    description = getErrorDescription(axiosError);
    expect(description).toBe("Error message");

    // if error object, return the message
    description = getErrorDescription(new Error("no no"));
    expect(description).toBe("no no");

    // if string, return the string
    description = getErrorDescription("error!");
    expect(description).toBe("error!");

    // if it's undefined, use a fallback
    description = getErrorDescription(null, "fallback");
    expect(description).toBe("fallback");

    // use default if nothing is defined
    description = getErrorDescription();
    expect(description).toBe("Something went wrong.");
  });
});
