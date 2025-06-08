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
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiStar } from "react-icons/fi";
import { useLocation } from "react-router-dom";

import { useDagServiceGetDags } from "openapi/queries";

import { FavoriteDagCard } from "./FavoriteDagCard";

const MAX_VISIBLE = 5;

export const FavoriteDags = () => {
  const queryClient = useQueryClient();
  const location = useLocation();

  const { t: translate } = useTranslation("dashboard");
  const { data: favorites } = useDagServiceGetDags({ favorites: true });

  useEffect(() => {
    void queryClient.refetchQueries({ queryKey: ["DagServiceGetDags"] });
  }, [location.key, queryClient]);

  const [showAll, setShowAll] = useState(false);

  const visibleFavorites = useMemo(() => {
    if (!favorites?.dags) {
      return [];
    }

    return showAll ? favorites.dags : favorites.dags.slice(0, MAX_VISIBLE);
  }, [favorites, showAll]);

  if (!favorites) {
    return undefined;
  }

  return (
    <Box>
      <Flex color="fg.muted" my={2}>
        <FiStar />
        <Heading ml={1} size="xs">
          {translate("favorite.favoriteDags")}
        </Heading>
      </Flex>

      {favorites.dags.length === 0 ? (
        <Text color="gray.500" fontSize="sm" ml={1}>
          {translate("favorite.noFavoriteDags")}
        </Text>
      ) : (
        <>
          <SimpleGrid alignItems="end" columnGap={1} columns={10} rowGap={4}>
            {visibleFavorites.map((dag) => (
              <FavoriteDagCard dagId={dag.dag_id} key={dag.dag_id} />
            ))}
          </SimpleGrid>

          {favorites.total_entries > MAX_VISIBLE && (
            <Box mt={2}>
              <Text
                _hover={{ textDecoration: "underline" }}
                as="span"
                color="fg.info"
                cursor="pointer"
                fontSize="sm"
                onClick={() => setShowAll(!showAll)}
              >
                {showAll ? translate("favorite.showLess") : translate("favorite.showMore")}
              </Text>
            </Box>
          )}
        </>
      )}
    </Box>
  );
};
