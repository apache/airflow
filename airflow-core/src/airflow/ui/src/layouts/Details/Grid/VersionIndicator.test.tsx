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
import { beforeEach, describe, expect, it } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { DagVersionIndicator } from "./VersionIndicator";

describe("DagVersionIndicator", () => {
  // The en wording for this key is the same sentence the hardcoded label used, so asserting on it
  // would pass with or without the change. Stand-in wording only matches a name read from the
  // bundle.
  beforeEach(() => {
    i18n.addResourceBundle(
      "en",
      "components",
      { versionDetails: { dagVersionIndicator: "Marks version {{version}}" } },
      true,
      true,
    );
  });

  it("takes the name of the version it marks from the locale", () => {
    render(<DagVersionIndicator dagVersionNumber={7} />, { wrapper: Wrapper });

    expect(screen.getByLabelText("Marks version 7")).toBeInTheDocument();
  });
});
