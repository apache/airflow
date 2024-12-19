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
import {
  Box,
  type ButtonProps,
  IconButton,
  useDisclosure,
} from "@chakra-ui/react";
import { type FC, useState } from "react";
import { FiRefreshCw } from "react-icons/fi";

import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { Button, Tooltip } from "src/components/ui";
import { useClearDagRun } from "src/queries/useClearRun";

import ClearRunDialog from "./ClearRunDialog";

type Props = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly withText?: boolean;
};

const ClearRunButton = ({ dagId, dagRunId, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  const [onlyFailed, setOnlyFailed] = useState(false);

  const [affectedTasks, setAffectedTasks] =
    useState<TaskInstanceCollectionResponse>({
      task_instances: [],
      total_entries: 0,
    });

  const ButtonComponent: FC<ButtonProps> = withText ? Button : IconButton;

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
    onSuccessDryRun: setAffectedTasks,
  });

  return (
    <Box>
      <Tooltip content="Clear Dag Run" disabled={Boolean(withText)}>
        <ButtonComponent
          colorPalette={withText ? undefined : "blue"}
          onClick={() => {
            onOpen();
            mutate({
              dagId,
              dagRunId,
              requestBody: { dry_run: true, only_failed: onlyFailed },
            });
          }}
          size={withText ? "md" : "sm"}
          variant={withText ? "outline" : "ghost"}
        >
          <FiRefreshCw />
          {withText ? "Clear Run" : ""}
        </ButtonComponent>
      </Tooltip>

      <ClearRunDialog
        affectedTasks={affectedTasks}
        dagId={dagId}
        dagRunId={dagRunId}
        isPending={isPending}
        mutate={mutate}
        onClose={onClose}
        onlyFailed={onlyFailed}
        open={open}
        setOnlyFailed={setOnlyFailed}
      />
    </Box>
  );
};

export default ClearRunButton;
