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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TaskName } from "./TaskName";

const LONG_LABEL = "mysql___d___dashboard___commerce___vbp___d___0";

describe("TaskName", () => {
  it("keeps the end of a long name out of the ellipsis", () => {
    render(<TaskName label={LONG_LABEL} />, { wrapper: BaseWrapper });

    // The tail is its own non-shrinking element, so only the start of the name gets cut.
    expect(screen.getByText("_vbp___d___0")).toBeInTheDocument();
    expect(screen.getByTitle(LONG_LABEL)).toHaveTextContent(LONG_LABEL);
  });

  it("keeps the end of a long group name out of the ellipsis", () => {
    render(<TaskName isGroup label={LONG_LABEL} />, { wrapper: BaseWrapper });

    expect(screen.getByText("_vbp___d___0")).toBeInTheDocument();
    expect(screen.getByTitle(LONG_LABEL)).toHaveTextContent(LONG_LABEL);
  });

  it("renders a short name in one piece", () => {
    render(<TaskName label="extract" />, { wrapper: BaseWrapper });

    expect(screen.getByText("extract")).toBeInTheDocument();
    expect(screen.getByTitle("extract")).toBeInTheDocument();
  });

  it("keeps the mapped count after the name", () => {
    render(<TaskName childCount={3} isMapped label={LONG_LABEL} />, { wrapper: BaseWrapper });

    expect(screen.getByTitle(LONG_LABEL)).toHaveTextContent(`${LONG_LABEL} [3]`);
  });
});
