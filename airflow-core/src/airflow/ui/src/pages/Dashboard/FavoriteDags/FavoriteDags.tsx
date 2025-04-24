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
import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import { FiStar } from "react-icons/fi";
import { FavoriteDagCard } from "./FavoriteDagCard";
import { fetchFavoriteDags } from "src/queries/useFavoriteDag";


export const FavoriteDags = () => {
    const { data: favorites } = fetchFavoriteDags();

    return (
        <Box>
            <Flex color="fg.muted" my={2}>
            <FiStar />
            <Heading ml={1} size="xs">
                Favorite Dags
            </Heading>
            </Flex>
            <HStack align="end">
              {favorites.map((dag_id) => (
                <FavoriteDagCard key={dag_id} dag_id={dag_id} />
              ))}
            </HStack>
        </Box>
    );
};
