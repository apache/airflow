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
import { useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetExtraLinks } from "openapi/queries";

export const ExtraLinks = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const mapIndexParam = searchParams.get("map_index");
  const mapIndex = parseInt(mapIndexParam ?? "-1", 10);

  const { data } = useTaskInstanceServiceGetExtraLinks({
    dagId,
    dagRunId: runId,
    mapIndex,
    taskId,
  });

  return data && Object.keys(data).length > 0 ? (
    <Box py={1}>
      <Heading size="sm"> Extra Links </Heading>
      <HStack gap={2} py={2}>
        {Object.entries(data).map(([key, value], _) =>
          value === null ? undefined : (
            <Button asChild colorPalette="blue" key={key} variant="surface">
              <a href={value} rel="noreferrer" target="_blank">
                {key}
              </a>
            </Button>
          ),
        )}
      </HStack>
    </Box>
  ) : undefined;
};
