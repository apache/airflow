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
import { useDisclosure } from "@chakra-ui/react";

import type { DAGResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { Button } from "../ui";
import RunBackfillModal from "./RunBackfillModal";

type Props = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
};

const BackfillIcon = () => (
  <svg
    fill="none"
    height="100"
    stroke="black"
    strokeWidth="5"
    viewBox="0 0 100 100"
    width="100"
    xmlns="http://www.w3.org/2000/svg"
  >
    {/* Grid structure */}
    <rect fill="white" height="80" strokeDasharray="20 10" width="20" x="10" y="10" />
    <rect fill="black" height="80" width="20" x="40" y="10" />
    <rect fill="black" height="80" width="20" x="70" y="10" />
  </svg>
);

const RunBackfillButton: React.FC<Props> = ({ dag }) => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <Box>
      <Button aria-label="Run Backfill" border="none" height={5} onClick={onOpen} variant="ghost">
        <BackfillIcon />
        Run Backfill
      </Button>
      <RunBackfillModal dag={dag} onClose={onClose} open={open} />
    </Box>
  );
};

export default RunBackfillButton;
