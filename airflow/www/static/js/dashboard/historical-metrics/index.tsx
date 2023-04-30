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
import { Card, CardBody, CardHeader, Flex, Heading } from "@chakra-ui/react";
import InfoTooltip from "src/components/InfoTooltip";

const HistoricalMetrics = () => (
  <Flex w="100%">
    <Card w="100%">
      <CardHeader>
        <Flex alignItems="center">
          <Heading size="md">Historical metrics</Heading>
          <InfoTooltip
            label="Based on historical data. You can adjust the period by setting a different start and end date filter."
            size={18}
          />
        </Flex>
      </CardHeader>
      <CardBody>This is the body</CardBody>
    </Card>
  </Flex>
);

export default HistoricalMetrics;
