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
  readonly addon?: React.ReactNode;
  readonly description?: React.ReactNode;
  readonly icon?: React.ReactElement;
  readonly indicator?: React.ReactNode | null;
  readonly indicatorPlacement?: "end" | "inside" | "start";
  readonly inputProps?: React.InputHTMLAttributes<HTMLInputElement>;
  readonly label?: React.ReactNode;
} & RadioCard.ItemProps;

export const RadioCardItem = forwardRef<HTMLInputElement, RadioCardItemProps>((props, ref) => {
  const {
    addon,
    description,
    icon,
    indicator = <RadioCard.ItemIndicator />,
    indicatorPlacement = "end",
    inputProps,
    label,
    ...rest
  } = props;

  const hasContent = label ?? description ?? icon;
  const shouldWrapContent = Boolean(indicator);

  return (
    <RadioCard.Item {...rest}>
      <RadioCard.ItemHiddenInput ref={ref} {...inputProps} />
      <RadioCard.ItemControl>
        {indicatorPlacement === "start" && indicator}
        {Boolean(hasContent) ? (
          shouldWrapContent ? (
            <RadioCard.ItemContent>
              {icon}
              {Boolean(label) ? <RadioCard.ItemText>{label}</RadioCard.ItemText> : undefined}
              {Boolean(description) ? (
                <RadioCard.ItemDescription>{description}</RadioCard.ItemDescription>
              ) : undefined}
              {indicatorPlacement === "inside" && indicator}
            </RadioCard.ItemContent>
          ) : (
            <>
              {icon}
              {Boolean(label) ? <RadioCard.ItemText>{label}</RadioCard.ItemText> : undefined}
              {Boolean(description) ? (
                <RadioCard.ItemDescription>{description}</RadioCard.ItemDescription>
              ) : undefined}
              {indicatorPlacement === "inside" && indicator}
            </>
          )
        ) : undefined}
        {indicatorPlacement === "end" && indicator}
      </RadioCard.ItemControl>
      {Boolean(addon) ? <RadioCard.ItemAddon>{addon}</RadioCard.ItemAddon> : undefined}
    </RadioCard.Item>
  );
});

export const RadioCardRoot = RadioCard.Root;
export const RadioCardLabel = RadioCard.Label;
export const RadioCardItemIndicator = RadioCard.ItemIndicator;
