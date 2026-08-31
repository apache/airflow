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
import { Accordion, HStack } from "@chakra-ui/react";
import { forwardRef } from "react";
import { LuChevronDown } from "react-icons/lu";

type AccordionItemTriggerProps = {
  readonly indicatorPlacement?: "end" | "start";
} & Accordion.ItemTriggerProps;

export const ItemTrigger = forwardRef<HTMLButtonElement, AccordionItemTriggerProps>((props, ref) => {
  const { children, indicatorPlacement = "end", ...rest } = props;

  return (
    <Accordion.ItemTrigger {...rest} ref={ref}>
      {indicatorPlacement === "start" && (
        <Accordion.ItemIndicator rotate={{ _open: "0deg", base: "-90deg" }}>
          <LuChevronDown />
        </Accordion.ItemIndicator>
      )}
      <HStack flex="1" gap="4" textAlign="start" width="full">
        {children}
      </HStack>
      {indicatorPlacement === "end" && (
        <Accordion.ItemIndicator>
          <LuChevronDown />
        </Accordion.ItemIndicator>
      )}
    </Accordion.ItemTrigger>
  );
});
