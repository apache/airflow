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
import { useCallback, useMemo } from "react";
import { FiStar } from "react-icons/fi";

import { useDagServiceGetFavoriteDags } from "openapi/queries";
import { useFavoriteDag } from "src/queries/useFavoriteDag";

import ActionButton from "../ui/ActionButton";

type FavoriteDagButtonProps = {
  readonly dagId: string;
  readonly withText?: boolean;
};

export const FavoriteDagButton = ({ dagId, withText = true }: FavoriteDagButtonProps) => {
  const { data: favorites, refetch } = useDagServiceGetFavoriteDags();

  const isFavorite = useMemo(
    () => favorites?.dags.some((fav) => fav.dag_id === dagId) ?? false,
    [favorites, dagId],
  );

  const { mutate: toggleFavorite } = useFavoriteDag({ dagId });

  const onToggle = useCallback(() => {
    toggleFavorite(
      {
        dagId,
        requestBody: {
          is_favorite: !isFavorite,
        },
      },
      {
        onSuccess: () => {
          void refetch();
        },
      },
    );
  }, [dagId, isFavorite, toggleFavorite, refetch]);

  return (
    <Box>
      <ActionButton
        actionName={isFavorite ? "Unfavorite Dag" : "Favorite Dag"}
        colorPalette="blue"
        icon={<FiStar style={{ fill: isFavorite ? "blue" : "none" }} />}
        onClick={onToggle}
        text={isFavorite ? "Unfavorite Dag" : "Favorite Dag"}
        variant="solid"
        withText={withText}
      />
    </Box>
  );
};
