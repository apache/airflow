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
import { HStack } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import type { ControllerRenderProps } from "react-hook-form";

import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";

import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "../ui/RadioCard";
import type { BackfillFormProps } from "./RunBackfillForm";

type Props = {
  readonly field: ControllerRenderProps<BackfillFormProps, "reprocess_behavior">;
  readonly translate: TFunction;
};

export const ReprocessBehaviorForm = ({ field, translate }: Props) => (
  <RadioCardRoot
    defaultValue={field.value}
    onChange={(event) => {
      field.onChange(event);
    }}
  >
    <RadioCardLabel fontSize="md" fontWeight="semibold" mb={3}>
      {translate("backfill.reprocessBehavior")}
    </RadioCardLabel>
    <HStack align="stretch">
      {reprocessBehaviors.map((item) => (
        <RadioCardItem
          colorPalette="blue"
          indicatorPlacement="start"
          key={item.value}
          label={translate(item.label)}
          value={item.value}
        />
      ))}
    </HStack>
  </RadioCardRoot>
);
