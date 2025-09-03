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
import { HStack, IconButton } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlignJustify, FiGrid } from "react-icons/fi";

import { Tooltip } from "src/components/ui";

type Display = "card" | "table";

type Props = {
  readonly display: Display;
  readonly setDisplay: (display: Display) => void;
};

export const ToggleTableDisplay = ({ display, setDisplay }: Props) => {
  const { t: translate } = useTranslation("components");

  return (
    <HStack colorPalette="brand" gap={1} pb={2}>
      <Tooltip content={translate("toggleCardView")}>
        <IconButton
          _hover={{ bg: "colorPalette.emphasized" }}
          aria-label={translate("toggleCardView")}
          bg={display === "card" ? "colorPalette.muted" : "bg"}
          borderColor="border.emphasized"
          borderWidth={1}
          color="colorPalette.fg"
          height={8}
          minWidth={8}
          onClick={() => setDisplay("card")}
          width={8}
        >
          <FiGrid />
        </IconButton>
      </Tooltip>
      <Tooltip content={translate("toggleTableView")}>
        <IconButton
          _hover={{ bg: "colorPalette.emphasized" }}
          aria-label={translate("toggleTableView")}
          bg={display === "table" ? "colorPalette.muted" : "bg"}
          borderColor="border.emphasized"
          borderWidth={1}
          color="colorPalette.fg"
          height={8}
          minWidth={8}
          onClick={() => setDisplay("table")}
          width={8}
        >
          <FiAlignJustify />
        </IconButton>
      </Tooltip>
    </HStack>
  );
};
