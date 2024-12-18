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
import { Box, Flex, Heading } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import {
  useTaskInstanceServiceGetTaskInstance,
  useTaskInstanceServiceGetTaskInstanceTries,
} from "openapi/queries";
import Time from "src/components/Time";
import { TrySelector } from "src/components/TrySelector";
import { Status } from "src/components/ui";

export const Details = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();

  const { data: taskInstance } = useTaskInstanceServiceGetTaskInstance({
    dagId,
    dagRunId: runId,
    taskId,
  });

  const finalTryNumber = taskInstance?.try_number ?? 1;

  const { data: taskInstanceTries } =
    useTaskInstanceServiceGetTaskInstanceTries({
      dagId,
      dagRunId: runId,
      mapIndex: taskInstance?.map_index ?? -1,
      taskId,
    });

  const [selectedTryNumber, setSelectedTryNumber] = useState(
    finalTryNumber || 1,
  );

  // update state if the final try number changes
  useEffect(() => {
    if (finalTryNumber) {
      setSelectedTryNumber(finalTryNumber);
    }
  }, [finalTryNumber]);

  const tryInstance = taskInstanceTries?.task_instances.find(
    (ti) => ti.try_number === selectedTryNumber,
  );

  const instance =
    selectedTryNumber !== finalTryNumber && finalTryNumber && finalTryNumber > 1
      ? tryInstance
      : taskInstance;

  return (
    <Box flexGrow={1} mt={3}>
      {Boolean(taskInstance) && (
        <TrySelector
          onSelectTryNumber={setSelectedTryNumber}
          selectedTryNumber={selectedTryNumber || finalTryNumber}
          taskInstance={taskInstance}
        />
      )}
    </Box>
  );
};
