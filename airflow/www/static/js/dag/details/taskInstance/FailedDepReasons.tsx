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
  failedDepReasons?: TaskInstanceAttributes["failedDepReasons"];
}

const FailedDepReasons = ({ failedDepReasons }: Props) => {
  if (!failedDepReasons) return null;
  return (
    <AccordionItem>
      <AccordionHeader>
        Dependencies Blocking Task From Getting Scheduled
      </AccordionHeader>
      <AccordionPanel>
        <Table variant="striped">
          <Tbody>
            {failedDepReasons.map((fdr: any) => (
              <Tr key={fdr[0]}>
                <Td>{fdr[0]}</Td>
                <Td>{fdr[1]}</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </AccordionPanel>
    </AccordionItem>
  );
};

export default FailedDepReasons;
