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

import {
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  DEFAULT_GRAPH_DIRECTION_KEY,
  DEFAULT_LANDING_PAGE_KEY,
  DEFAULT_TASK_INSTANCE_TAB_KEY,
} from "src/constants/localStorage";
import { BaseWrapper } from "src/utils/Wrapper";

import { Settings } from "./Settings";

beforeAll(async () => {
  await i18n.use(initReactI18next).init({
    defaultNS: "common",
    fallbackLng: "en",
    interpolation: { escapeValue: false },
    lng: "en",
    ns: ["common", "components", "dag", "dags"],
    resources: {
      en: {
        common: {
          settings: {
            clearing: {
              preventRunningTask: { helper: "helper", label: "Prevent clearing running tasks" },
              runSelection: { helper: "helper", label: "Default run clear selection" },
              taskSelection: { helper: "helper", label: "Default task clear selection" },
              title: "Clearing",
            },
            description: "browser only",
            general: {
              landingPage: {
                helper: "helper",
                label: "Landing page",
                options: { dags: "Dags", dashboard: "DASHBOARD-OPT" },
              },
              title: "General",
            },
            graph: {
              defaultDirection: { helper: "helper", label: "Default graph direction" },
              title: "Graph",
            },
            marking: {
              taskSelection: { helper: "helper", label: "Default mark selection" },
              title: "Marking",
            },
            taskInstance: {
              defaultTab: { helper: "helper", label: "Default task instance tab" },
              title: "Task Instance",
            },
            title: "Settings",
          },
        },
        components: {
          graph: {
            directionDown: "DOWN-LABEL",
            directionLeft: "LEFT-LABEL",
            directionRight: "RIGHT-LABEL",
            directionUp: "UP-LABEL",
          },
        },
        dag: {
          tabs: {
            assetEvents: "Asset Events",
            auditLog: "Audit Log",
            code: "Code",
            details: "DETAILS-TAB",
            logs: "Logs",
            renderedTemplates: "Rendered Templates",
            xcom: "XCom",
          },
        },
        dags: {
          runAndTaskActions: {
            options: {
              downstream: "DOWNSTREAM-OPT",
              existingTasks: "EXISTING-OPT",
              future: "FUTURE-OPT",
              onlyFailed: "ONLY-FAILED-OPT",
              past: "PAST-OPT",
              queueNew: "QUEUE-NEW-OPT",
              upstream: "UPSTREAM-OPT",
            },
          },
        },
      },
    },
  });
});

afterEach(() => {
  localStorage.clear();
});

describe("Settings page", () => {
  it("renders the graph, clearing and marking settings", () => {
    render(<Settings />, { wrapper: BaseWrapper });

    expect(screen.getByText("Settings")).toBeInTheDocument();

    // Selects and the switch expose test ids.
    for (const testId of [
      "default-landing-page",
      "default-graph-direction",
      "default-task-instance-tab",
      "clear-prevent-running-task",
    ]) {
      expect(screen.getByTestId(testId)).toBeInTheDocument();
    }

    // Toggle settings are identified by their labels.
    expect(screen.getByText("Default run clear selection")).toBeInTheDocument();
    expect(screen.getByText("Default task clear selection")).toBeInTheDocument();
    expect(screen.getByText("Default mark selection")).toBeInTheDocument();

    // The prevent-running switch defaults to on.
    expect(screen.getByTestId("clear-prevent-running-task")).toHaveAttribute("data-state", "checked");
  });

  it("reflects stored values in the controls", () => {
    localStorage.setItem(DEFAULT_GRAPH_DIRECTION_KEY, JSON.stringify("DOWN"));
    localStorage.setItem(CLEAR_PREVENT_RUNNING_TASK_KEY, JSON.stringify(false));

    localStorage.setItem(DEFAULT_TASK_INSTANCE_TAB_KEY, JSON.stringify("details"));
    localStorage.setItem(DEFAULT_LANDING_PAGE_KEY, JSON.stringify("dashboard"));

    render(<Settings />, { wrapper: BaseWrapper });

    expect(screen.getByTestId("default-landing-page")).toHaveTextContent("DASHBOARD-OPT");
    expect(screen.getByTestId("default-graph-direction")).toHaveTextContent("DOWN-LABEL");
    expect(screen.getByTestId("default-task-instance-tab")).toHaveTextContent("DETAILS-TAB");
    expect(screen.getByTestId("clear-prevent-running-task")).toHaveAttribute("data-state", "unchecked");
  });
});
