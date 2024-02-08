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

import React from "react";
import {
  Table,
  Tbody,
  Tr,
  Td,
  AccordionItem,
  AccordionPanel,
} from "@chakra-ui/react";

import type { TaskInstanceAttributes } from "src/types";

import AccordionHeader from "src/components/AccordionHeader";

interface Props {
  tiAttrs?: TaskInstanceAttributes["tiAttrs"];
}

const Attributes = ({ tiAttrs }: Props) => {
  if (!tiAttrs) return null;
  return (
    <AccordionItem>
      <AccordionHeader>Task Instance Attributes</AccordionHeader>
      <AccordionPanel>
        <Table variant="striped">
          <Tbody>
            {Object.keys(tiAttrs).map((key) => {
              let value = tiAttrs[key] || "";
              if (typeof value === "object")
                value = JSON.stringify(value, null, 2);
              return (
                <Tr key={key}>
                  <Td>{key}</Td>
                  <Td>{value}</Td>
                </Tr>
              );
            })}
          </Tbody>
        </Table>
      </AccordionPanel>
    </AccordionItem>
  );
};

export default Attributes;
