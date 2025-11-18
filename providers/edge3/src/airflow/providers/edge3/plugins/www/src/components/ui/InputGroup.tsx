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

/* eslint-disable @typescript-eslint/strict-boolean-expressions */

/* eslint-disable @typescript-eslint/no-unsafe-argument */
import type { BoxProps, InputElementProps } from "@chakra-ui/react";
import { Group, InputElement } from "@chakra-ui/react";
import { cloneElement, forwardRef } from "react";

export type InputGroupProps = {
  readonly children: React.ReactElement;
  readonly endElement?: React.ReactNode;
  readonly endElementProps?: InputElementProps;
  readonly startElement?: React.ReactNode;
  readonly startElementProps?: InputElementProps;
} & BoxProps;

export const InputGroup = forwardRef<HTMLDivElement, InputGroupProps>((props, ref) => {
  const { children, endElement, endElementProps, startElement, startElementProps, ...rest } = props;

  return (
    <Group ref={ref} {...rest}>
      {startElement ? (
        <InputElement pointerEvents="none" {...startElementProps}>
          {startElement}
        </InputElement>
      ) : undefined}
      {cloneElement(children, {
        ...(startElement && { ps: "calc(var(--input-height) - 6px)" }),
        ...(endElement && { pe: "calc(var(--input-height) - 6px)" }),
        ...children.props,
      })}
      {endElement ? (
        <InputElement placement="end" {...endElementProps}>
          {endElement}
        </InputElement>
      ) : undefined}
    </Group>
  );
});
