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
import { ButtonGroup, IconButton } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlignJustify, FiGrid } from "react-icons/fi";

type Display = "card" | "table";

type Props = {
  readonly display: Display;
  readonly setDisplay: (display: Display) => void;
};

export const ToggleTableDisplay = ({ display, setDisplay }: Props) => {
  const { t: translate } = useTranslation("components");

  return (
    <ButtonGroup attached colorPalette="brand" pb={2} size="sm" variant="outline">
      <IconButton
        aria-label={translate("toggleCardView")}
        bg={display === "card" ? "colorPalette.muted" : undefined}
        onClick={() => setDisplay("card")}
        title={translate("toggleCardView")}
        variant={display === "card" ? "solid" : "outline"}
      >
        <FiGrid />
      </IconButton>
      <IconButton
        aria-label={translate("toggleTableView")}
        bg={display === "table" ? "colorPalette.muted" : undefined}
        onClick={() => setDisplay("table")}
        title={translate("toggleTableView")}
        variant={display === "table" ? "solid" : "outline"}
      >
        <FiAlignJustify />
      </IconButton>
    </ButtonGroup>
  );
};
