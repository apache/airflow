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
import { Box, IconButton } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import { FiPlay } from "react-icons/fi";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import TriggerDAGModal from "./TriggerDAGModal";

type Props = {
  readonly dag: DAGWithLatestDagRunsResponse;
};

const TriggerDAGIconButton: React.FC<Props> = ({ dag }) => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <Box>
      <IconButton
        aria-label={`Trigger ${dag.dag_display_name}`}
        colorPalette="blue"
        onClick={onOpen}
        size="xs"
        variant="ghost"
      >
        <FiPlay />
      </IconButton>

      <TriggerDAGModal
        dagDisplayName={dag.dag_display_name}
        dagId={dag.dag_id}
        isPaused={dag.is_paused}
        onClose={onClose}
        open={open}
      />
    </Box>
  );
};

export default TriggerDAGIconButton;
