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
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { afterEach, beforeAll, describe, expect, it } from "vitest";

import { DEFAULT_GRAPH_DIRECTION_KEY, directionKey } from "src/constants/localStorage";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import { DirectionDropdown } from "./DirectionDropdown";

beforeAll(async () => {
  await i18n.use(initReactI18next).init({
    defaultNS: "components",
    fallbackLng: "en",
    interpolation: { escapeValue: false },
    lng: "en",
    ns: ["components", "dag"],
    resources: {
      en: {
        components: {
          graph: {
            directionDown: "DOWN-LABEL",
            directionLeft: "LEFT-LABEL",
            directionRight: "RIGHT-LABEL",
            directionUp: "UP-LABEL",
          },
        },
        dag: { panel: { graphDirection: { label: "Direction" } } },
      },
    },
  });
});

afterEach(() => {
  localStorage.clear();
});

describe("DirectionDropdown", () => {
  it("initializes from the global default when no per-graph direction is stored", () => {
    localStorage.setItem(DEFAULT_GRAPH_DIRECTION_KEY, JSON.stringify("DOWN"));

    render(<DirectionDropdown graphId="test-dag" />, { wrapper: ChakraWrapper });

    expect(screen.getByRole("combobox")).toHaveTextContent("DOWN-LABEL");
  });

  it("uses the stored per-graph direction over the global default", () => {
    localStorage.setItem(DEFAULT_GRAPH_DIRECTION_KEY, JSON.stringify("DOWN"));
    localStorage.setItem(directionKey("test-dag"), JSON.stringify("LEFT"));

    render(<DirectionDropdown graphId="test-dag" />, { wrapper: ChakraWrapper });

    expect(screen.getByRole("combobox")).toHaveTextContent("LEFT-LABEL");
  });
});
