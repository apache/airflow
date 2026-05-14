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
import { IconButton } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiChevronUp } from "react-icons/fi";

import { Tooltip } from "src/components/ui";
import { getMetaKey } from "src/utils";

export const ScrollToButton = ({
  direction,
  onClick,
}: {
  readonly direction: "bottom" | "top";
  readonly onClick: () => void;
}) => {
  const { t: translate } = useTranslation("common");

  return (
    <Tooltip
      closeDelay={100}
      content={translate("scroll.tooltip", {
        direction: translate(`scroll.direction.${direction}`),
        hotkey: `${getMetaKey()}+${direction === "bottom" ? "↓" : "↑"}`,
      })}
      openDelay={100}
    >
      <IconButton
        _ltr={{ left: "auto", right: 4 }}
        _rtl={{ left: 4, right: "auto" }}
        aria-label={translate(`scroll.direction.${direction}`)}
        bg="bg.panel"
        bottom={direction === "bottom" ? 4 : 14}
        onClick={onClick}
        position="absolute"
        rounded="full"
        size="xs"
        variant="outline"
      >
        {direction === "bottom" ? <FiChevronDown /> : <FiChevronUp />}
      </IconButton>
    </Tooltip>
  );
};
