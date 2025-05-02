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
import { ReactFlowProvider } from "@xyflow/react";
import { LuChartColumn } from "react-icons/lu";
import { MdOutlineEventNote, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useTaskServiceGetTask } from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";

import { Header } from "./Header";

const tabs = [
  { icon: <LuChartColumn />, label: "Overview", value: "" },
  { icon: <MdOutlineTask />, label: "Task Instances", value: "task_instances" },
  { icon: <MdOutlineEventNote />, label: "Events", value: "events" },
];

export const Task = () => {
  const { dagId = "", taskId = "" } = useParams();

  const { data: task, error, isLoading } = useTaskServiceGetTask({ dagId, taskId });

  const {
    data: dag,
    error: dagError,
    isLoading: isDagLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  return (
    <ReactFlowProvider>
      <DetailsLayout dag={dag} error={error ?? dagError} isLoading={isLoading || isDagLoading} tabs={tabs}>
        {task === undefined ? undefined : <Header task={task} />}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
