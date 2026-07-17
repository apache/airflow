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
import { Box } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { MdOutlineTask } from "react-icons/md";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import ClearTaskInstanceDialog from "src/components/Clear/TaskInstance/ClearTaskInstanceDialog";
import { DagVersion } from "src/components/DagVersion";
import { HeaderCard } from "src/components/HeaderCard";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import NotePreview from "src/components/NotePreview";
import Time from "src/components/Time";
import { useTaskInstanceNote } from "src/queries/useTaskInstanceNote";
import { getDuration, renderDuration } from "src/utils";

export const Header = ({ taskInstance }: { readonly taskInstance: TaskInstanceResponse }) => {
  const { t: translate } = useTranslation();
  const { isPending, note, onOpen, onSave, setNote } = useTaskInstanceNote(taskInstance);

  const stats = [
    { label: translate("task.operator"), value: taskInstance.operator_name },
    ...(taskInstance.map_index > -1
      ? [{ label: translate("mapIndex"), value: taskInstance.rendered_map_index }]
      : []),
    ...(taskInstance.try_number > 1
      ? [{ label: translate("tryNumber"), value: taskInstance.try_number }]
      : []),
    { label: translate("startDate"), value: <Time datetime={taskInstance.start_date} /> },
    { label: translate("endDate"), value: <Time datetime={taskInstance.end_date} /> },
    ...(Boolean(taskInstance.start_date)
      ? [
          {
            label: translate("duration"),
            value: Boolean(taskInstance.duration)
              ? renderDuration(taskInstance.duration)
              : getDuration(taskInstance.start_date, taskInstance.end_date),
          },
        ]
      : []),
    {
      label: translate("taskInstance.dagVersion"),
      value: <DagVersion version={taskInstance.dag_version} />,
    },
  ];

  // Stable dialog state at header/page level
  const [clearOpen, setClearOpen] = useState(false);

  return (
    <Box>
      <HeaderCard
        actions={
          <>
            <ClearTaskInstanceButton
              isHotkeyEnabled
              onOpen={() => setClearOpen(true)}
              taskInstance={taskInstance}
            />
            <MarkTaskInstanceAsButton isHotkeyEnabled taskInstance={taskInstance} />
          </>
        }
        icon={<MdOutlineTask />}
        state={taskInstance.state}
        stats={stats}
        title={`${taskInstance.task_display_name}${taskInstance.map_index > -1 ? ` [${taskInstance.rendered_map_index ?? taskInstance.map_index}]` : ""}`}
      />
      <NotePreview
        header={translate("note.taskInstance")}
        isPending={isPending}
        note={note}
        onOpen={onOpen}
        onSave={onSave}
        setNote={setNote}
      />
      <ClearTaskInstanceDialog
        onClose={() => setClearOpen(false)}
        open={clearOpen}
        taskInstance={taskInstance}
      />
    </Box>
  );
};
