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
import { Icon } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiStar } from "react-icons/fi";

import { ButtonGroupToggle } from "src/components/ui/ButtonGroupToggle";

type FavoriteValue = "all" | "false" | "true";

type Props = {
  readonly onChange: (value: FavoriteValue) => void;
  readonly value: FavoriteValue;
};

const StarIcon = ({ filled, isSelected }: { readonly filled: boolean; readonly isSelected: boolean }) => (
  <Icon asChild color={isSelected ? "brand.contrast" : "brand.solid"}>
    <FiStar style={filled ? { fill: "currentColor" } : undefined} />
  </Icon>
);

const renderFavoriteLabel =
  (text: string, filled: boolean) =>
  (isSelected: boolean): React.ReactNode => (
    <>
      <StarIcon filled={filled} isSelected={isSelected} />
      {text}
    </>
  );

export const FavoriteFilter = ({ onChange, value }: Props) => {
  const { t: translate } = useTranslation("dags");

  return (
    <ButtonGroupToggle<FavoriteValue>
      onChange={onChange}
      options={[
        { label: translate("filters.favorite.all"), value: "all" },
        {
          label: renderFavoriteLabel(translate("filters.favorite.favorite"), true),
          value: "true",
        },
        {
          label: renderFavoriteLabel(translate("filters.favorite.unfavorite"), false),
          value: "false",
        },
      ]}
      value={value}
    />
  );
};
