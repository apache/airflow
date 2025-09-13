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
import { Box, Flex, Heading, SimpleGrid, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiStar } from "react-icons/fi";

import { useDagServiceGetDagsUi } from "openapi/queries";

import { FavoriteDagCard } from "./FavoriteDagCard";

export const FavoriteDags = () => {
  const { t: translate } = useTranslation("dashboard");
  const LIMIT = 10;
  const { data: favorites } = useDagServiceGetDagsUi({ isFavorite: true, limit: LIMIT });

  if (!favorites) {
    return undefined;
  }

  return (
    <Box>
      <Flex color="fg.muted" my={2}>
        <FiStar />
        <Heading ml={1} size="xs">
          {translate("favorite.favoriteDags", { count: LIMIT })}
        </Heading>
      </Flex>

      {favorites.dags.length === 0 ? (
        <Text color="fg.muted" fontSize="sm" ml={1}>
          {translate("favorite.noFavoriteDags")}
        </Text>
      ) : (
        <SimpleGrid alignItems="end" columnGap={1} columns={10} rowGap={4}>
          {favorites.dags.map((dag) => (
            <FavoriteDagCard
              dagId={dag.dag_id}
              dagName={dag.dag_display_name}
              key={dag.dag_id}
              latestRuns={dag.latest_dag_runs}
            />
          ))}
        </SimpleGrid>
      )}
    </Box>
  );
};
