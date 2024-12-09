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
import { Box, Button, Heading } from "@chakra-ui/react";
import { FiChevronsLeft } from "react-icons/fi";
import { useParams, Link as RouterLink } from "react-router-dom";

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar, Status } from "src/components/ui";

export const Run = () => {
  const { dagId = "", runId = "" } = useParams();

  const {
    data: dagRun,
    error,
    isLoading,
  } = useDagRunServiceGetDagRun({
    dagId,
    dagRunId: runId,
  });

  return (
    <Box>
      <Button asChild colorPalette="blue" variant="ghost">
        <RouterLink to={`/dags/${dagId}`}>
          <FiChevronsLeft />
          Back to Dag
        </RouterLink>
      </Button>
      <Heading>
        {dagId}.{runId}
      </Heading>
      <ErrorAlert error={error} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      {dagRun === undefined ? undefined : (
        <Status state={dagRun.state}>{dagRun.state}</Status>
      )}
    </Box>
  );
};
