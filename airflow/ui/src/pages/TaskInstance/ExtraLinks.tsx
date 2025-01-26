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
import { Box, Button, Heading, HStack } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetExtraLinks } from "openapi/queries";

export const ExtraLinks = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const { data, isLoading } = useTaskInstanceServiceGetExtraLinks({ dagId, dagRunId: runId, taskId });

  return !isLoading && data !== undefined && Object.keys(data).length > 0 ? (
    <Box py={2}>
      <Heading size="sm"> Extra Links </Heading>
      <HStack gap={2} py={2}>
        {Object.keys(data).map((key) =>
          data[key] === null ? undefined : (
            <Button asChild colorPalette="blue" key={key}>
              <a href={data[key]} rel="noreferrer" target="_blank">
                {key}
              </a>
            </Button>
          ),
        )}
      </HStack>
    </Box>
  ) : undefined;
};
