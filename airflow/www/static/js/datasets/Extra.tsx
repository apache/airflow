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
  TableContainer,
  Table,
  Thead,
  Tbody,
  Tr,
  Td,
  Th,
} from "@chakra-ui/react";
import type { Dataset, DatasetEvent } from "src/types/api-generated";

type Props = {
  extra: Dataset["extra"] | DatasetEvent["extra"];
};

const Extra = ({ extra }: Props) =>
  extra ? (
    <TableContainer bg="gray.100" my={2}>
      <Table size="sm">
        <Thead>
          <Tr>
            <Th>Key</Th>
            <Th>Value</Th>
          </Tr>
        </Thead>
        <Tbody>
          {Object.entries(extra).map(([key, value]) => (
            <Tr key={key}>
              <Td>{key}</Td>
              <Td>{JSON.stringify(value)}</Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </TableContainer>
  ) : null;

export default Extra;
