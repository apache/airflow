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
import { useTranslation } from "react-i18next";
import { FiStar } from "react-icons/fi";

import { useToggleFavoriteDag } from "src/queries/useToggleFavoriteDag";

import ActionButton from "../ui/ActionButton";

type FavoriteDagButtonProps = {
  readonly dagId: string;
  readonly isFavorite?: boolean;
  readonly withText?: boolean;
};

export const FavoriteDagButton = ({ dagId, isFavorite = false, withText = true }: FavoriteDagButtonProps) => {
  const { t: translate } = useTranslation("dags");

  const { isLoading, toggleFavorite } = useToggleFavoriteDag(dagId);

  const onToggle = () => toggleFavorite(isFavorite);

  return (
    <Box>
      <ActionButton
        actionName={isFavorite ? translate("unfavoriteDag") : translate("favoriteDag")}
        icon={
          <FiStar
            style={{
              fill: isFavorite ? "var(--chakra-colors-brand-solid)" : "none",
              stroke: "var(--chakra-colors-brand-solid)",
            }}
          />
        }
        loading={isLoading}
        onClick={onToggle}
        text={isFavorite ? translate("unfavoriteDag") : translate("favoriteDag")}
        withText={withText}
      />
    </Box>
  );
};
