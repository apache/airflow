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
import "@testing-library/jest-dom";
import { act, render, screen } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { beforeAll, describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { ImportErrorCount } from "./ImportErrorCount";

describe("ImportErrorCount", () => {
  beforeAll(async () => {
    await i18n.use(initReactI18next).init({ lng: "en", resources: { de: {}, en: {} } });
  });

  it("groups thousands in the count and regroups on a language switch", async () => {
    render(<ImportErrorCount count={1234} />, { wrapper: Wrapper });

    expect(screen.getByText("1,234")).toBeInTheDocument();

    await act(async () => {
      await i18n.changeLanguage("de");
    });

    expect(screen.getByText("1.234")).toBeInTheDocument();
  });
});
