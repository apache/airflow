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
import { Button, ButtonGroup, Icon } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiStar } from "react-icons/fi";

type Props = {
  readonly onFavoriteChange: React.MouseEventHandler<HTMLButtonElement>;
  readonly showFavorites: string | null;
};

export const FavoriteFilter = ({ onFavoriteChange, showFavorites }: Props) => {
  const { t: translate } = useTranslation("dags");

  const currentValue = showFavorites ?? "all";

  return (
    <ButtonGroup attached size="sm" variant="outline">
      <Button
        bg={currentValue === "all" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onFavoriteChange}
        value="all"
        variant={currentValue === "all" ? "solid" : "outline"}
      >
        {translate("filters.favorite.all")}
      </Button>
      <Button
        bg={currentValue === "true" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onFavoriteChange}
        value="true"
        variant={currentValue === "true" ? "solid" : "outline"}
      >
        <Icon asChild color="brand.solid">
          <FiStar style={{ fill: "currentColor" }} />
        </Icon>
        {translate("filters.favorite.favorite")}
      </Button>
      <Button
        bg={currentValue === "false" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onFavoriteChange}
        value="false"
        variant={currentValue === "false" ? "solid" : "outline"}
      >
        <Icon asChild color="fg.muted">
          <FiStar />
        </Icon>
        {translate("filters.favorite.unfavorite")}
      </Button>
    </ButtonGroup>
  );
};
