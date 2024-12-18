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
import { Box, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { FiRefreshCw } from "react-icons/fi";

import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { Button } from "src/components/ui";
import { useClearDagRun } from "src/queries/useClearRun";

import ClearRunDialog from "./ClearRunDialog";

type Props = {
  readonly dagId: string;
  readonly dagRunId: string;
};

const ClearRunButton = ({ dagId, dagRunId }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  const [onlyFailed, setOnlyFailed] = useState(false);

  const [affectedTasks, setAffectedTasks] =
    useState<TaskInstanceCollectionResponse | null>();

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
    onSuccessDryRun: setAffectedTasks,
  });

  return (
    <Box>
      <Button
        onClick={() => {
          onOpen();
          mutate({
            dagId,
            dagRunId,
            requestBody: { dry_run: true, only_failed: onlyFailed },
          });
        }}
        variant="outline"
      >
        <FiRefreshCw height={5} width={5} />
        Clear Run
      </Button>

      <ClearRunDialog
        affectedTasks={affectedTasks as TaskInstanceCollectionResponse}
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
