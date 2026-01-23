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
import { RadioCard } from "@chakra-ui/react";
import { forwardRef } from "react";

type RadioCardItemProps = {
  readonly description?: React.ReactNode;
  readonly indicatorPlacement?: "end" | "start";
  readonly label?: React.ReactNode;
} & RadioCard.ItemProps;

export const RadioCardItem = forwardRef<HTMLInputElement, RadioCardItemProps>((props, ref) => {
  const { description, indicatorPlacement = "end", label, ...rest } = props;

  return (
    <RadioCard.Item {...rest}>
      <RadioCard.ItemHiddenInput ref={ref} />
      <RadioCard.ItemControl>
        {indicatorPlacement === "start" && <RadioCard.ItemIndicator />}
        <RadioCard.ItemContent>
          {Boolean(label) ? <RadioCard.ItemText>{label}</RadioCard.ItemText> : undefined}
          {Boolean(description) ? (
            <RadioCard.ItemDescription>{description}</RadioCard.ItemDescription>
          ) : undefined}
        </RadioCard.ItemContent>
        {indicatorPlacement === "end" && <RadioCard.ItemIndicator />}
      </RadioCard.ItemControl>
    </RadioCard.Item>
  );
});

export const RadioCardRoot = RadioCard.Root;
export const RadioCardLabel = RadioCard.Label;
