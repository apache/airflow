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
import type { ComponentProps } from "react";

import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { expect, vi } from "vitest";

import TriggerDAGForm from "./TriggerDAGForm";

export const dagParams = {
  paramsDict: {
    message: {
      description: "Message",
      schema: {
        title: "Message",
        type: "string",
      },
      value: "Hello",
    },
  },
};

export const prefillConfig = { conf: { key: "value" }, logicalDate: undefined, runId: "prior_run" };

export const buildForm = (props: Partial<ComponentProps<typeof TriggerDAGForm>>) => (
  <TriggerDAGForm
    dagId="example_partitioned_dag"
    error={undefined}
    hasSchedule={false}
    isPartitioned
    isPaused={false}
    isPending={false}
    onSubmitTrigger={vi.fn()}
    open
    {...props}
  />
);

export const RECENT_CONF = JSON.stringify({ message: "From recent" }, undefined, 2);

export const selectRecentConfig = async () => {
  fireEvent.click(within(screen.getByTestId("recent-config-select")).getByRole("combobox"));
  await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());
  fireEvent.click(screen.getByText("run_recent"));
};

export const SUGGESTED_HELP = "components:triggerDag.partitionKeySuggestedHelp";
export const PLAIN_HELP = "components:triggerDag.partitionKeyHelp";

export const getPartitionKeyInput = (container: HTMLElement) => {
  const input = container.querySelector<HTMLInputElement>('input[name="partitionKey"]');

  if (input === null) {
    throw new Error("Expected the partition key input to be rendered");
  }

  return input;
};
