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
import { HStack, IconButton } from "@chakra-ui/react";
import { MdOutlineOpenInFull } from "react-icons/md";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import { Button } from "src/components/ui";

type Props = {
  readonly isFullscreen?: boolean;
  readonly onSelectTryNumber: (tryNumber: number) => void;
  readonly taskInstance?: TaskInstanceResponse;
  readonly toggleFullscreen: () => void;
  readonly toggleWrap: () => void;
  readonly tryNumber?: number;
  readonly wrap: boolean;
};

export const TaskLogHeader = ({
  isFullscreen = false,
  onSelectTryNumber,
  taskInstance,
  toggleFullscreen,
  toggleWrap,
  tryNumber,
  wrap,
}: Props) => (
  <HStack justifyContent="space-between" mb={2}>
    {taskInstance === undefined || tryNumber === undefined || taskInstance.try_number <= 1 ? (
      <div />
    ) : (
      <TaskTrySelect
        onSelectTryNumber={onSelectTryNumber}
        selectedTryNumber={tryNumber}
        taskInstance={taskInstance}
      />
    )}
    <HStack>
      <Button aria-label={wrap ? "Unwrap" : "Wrap"} bg="bg.panel" onClick={toggleWrap} variant="outline">
        {wrap ? "Unwrap" : "Wrap"}
      </Button>
      {!isFullscreen && (
        <IconButton aria-label="Full screen" bg="bg.panel" onClick={toggleFullscreen} variant="outline">
          <MdOutlineOpenInFull />
        </IconButton>
      )}
    </HStack>
  </HStack>
);
