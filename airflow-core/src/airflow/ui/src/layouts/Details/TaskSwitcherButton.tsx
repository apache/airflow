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
import { type ReactNode, useState } from "react";

import { useTranslation } from "react-i18next";

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";

import { SearchTasks } from "./SearchTasks";

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly isCurrent: boolean;
  readonly runId: string | undefined;
  readonly shape: CrumbShape;
  readonly taskId: string;
  readonly to?: string;
};

/**
 * The task level of the breadcrumb, with a search over the Dag's tasks behind its chevron.
 *
 * Within a run, each task carries its state in that run, and picking one keeps the run, the open
 * tab where the picked task has it too, and the pass of a loop. Without a run, it goes to the task
 * across runs, on the same tab.
 */
export const TaskSwitcherButton = ({ children, dagId, isCurrent, runId, shape, taskId, to }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);

  return (
    <CrumbSwitcher
      isCurrent={isCurrent}
      label={translate("switchTask")}
      onOpenChange={setOpen}
      open={open}
      search={
        <SearchTasks currentTaskId={taskId} dagId={dagId} onClose={() => setOpen(false)} runId={runId} />
      }
      shape={shape}
      testId="switch-task"
      to={to}
    >
      {children}
    </CrumbSwitcher>
  );
};
