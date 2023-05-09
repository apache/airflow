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
import LogLink from "./LogLink";

describe("Test LogLink Component.", () => {
  test("Internal Link", () => {
    const tryNumber = 1;
    const { getByText, container } = render(
      <LogLink
        tryNumber={tryNumber}
        dagId="dummyDagId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
        isInternal
      />
    );

    expect(getByText("Download")).toBeDefined();
    const linkElement = container.querySelector("a");
    expect(linkElement).toBeDefined();
    expect(linkElement).not.toHaveAttribute("target");
    expect(
      linkElement?.href.includes(
        `?dag_id=dummyDagId&task_id=dummyTaskId&execution_date=2020%3A01%3A01T01%3A00%2B00%3A00&map_index=-1&format=file&try_number=${tryNumber}`
      )
    ).toBeTruthy();
  });

  test("External Link", () => {
    const tryNumber = 1;
    const { getByText, container } = render(
      <LogLink
        tryNumber={tryNumber}
        dagId="dummyDagId"
        taskId="dummyTaskId"
        executionDate="2020:01:01T01:00+00:00"
      />
    );

    expect(getByText(tryNumber)).toBeDefined();
    const linkElement = container.querySelector("a");
    expect(linkElement).toBeDefined();
    expect(linkElement).toHaveAttribute("target", "_blank");
    expect(
      linkElement?.href.includes(
        `?dag_id=dummyDagId&task_id=dummyTaskId&execution_date=2020%3A01%3A01T01%3A00%2B00%3A00&map_index=-1&try_number=${tryNumber}`
      )
    ).toBeTruthy();
  });
});
