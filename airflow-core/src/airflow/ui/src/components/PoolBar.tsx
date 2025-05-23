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
import { Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/components/ui";
import { type Slots, slotConfigs } from "src/utils/slots";

export const PoolBar = ({
  pool,
  poolsWithSlotType,
  totalSlots,
}: {
  readonly pool: Slots;
  readonly poolsWithSlotType?: Slots;
  readonly totalSlots: number;
}) => {
  const { t: translate } = useTranslation("common");

  return (
    <>
      {slotConfigs.map(({ color, icon, key }) => {
        const slotValue = pool[key];
        const flexValue = slotValue / totalSlots || 0;

        if (flexValue === 0) {
          return undefined;
        }

        const slotType = key.replace("_slots", "");
        const poolCount = poolsWithSlotType ? poolsWithSlotType[key] : 0;
        const tooltipContent = `${translate(`pools.${slotType}`)}: ${slotValue} (${poolCount} ${translate("pools.pools", { count: poolCount })})`;

        return (
          <Tooltip content={tooltipContent} key={key}>
            <Flex
              alignItems="center"
              bg={`${color}.solid`}
              color="white"
              flex={flexValue}
              gap={1}
              h="100%"
              justifyContent="center"
              px={1}
              py={0.5}
              textAlign="center"
            >
              {icon}
              {slotValue}
            </Flex>
          </Tooltip>
        );
      })}
    </>
  );
};
