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
import { Box, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { RecentRuns } from "src/pages/DagsList/RecentRuns";

type FavoriteDagProps = {
  readonly dagId: string;
  readonly dagName: string;
  readonly latestRuns: Array<DAGRunResponse>;
};

export const FavoriteDagCard = ({ dagId, dagName, latestRuns }: FavoriteDagProps) => {
  const { t: translate } = useTranslation("dashboard");

  return (
    <Box
      borderRadius="md"
      borderWidth="1px"
      display="flex"
      flexDirection="column"
      height="100%"
      justifyContent="center"
      overflow="hidden"
      px={4}
      py={3}
      width="100%"
    >
      <VStack>
        {latestRuns.length > 0 ? (
          <RecentRuns latestRuns={latestRuns} />
        ) : (
          <Text color="fg.muted" fontSize="sm" overflowWrap="anywhere" textAlign="center">
            {translate("favorite.noDagRuns")}
          </Text>
        )}
        <Link to={`/dags/${dagId}`}>
          <Text
            _hover={{ textDecoration: "underline" }}
            fontSize="sm"
            overflowWrap="anywhere"
            textAlign="center"
          >
            {dagName}
          </Text>
        </Link>
      </VStack>
    </Box>
  );
};
