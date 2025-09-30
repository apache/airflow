/* eslint-disable i18next/no-literal-string */

 

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
import { Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";

import { Switch } from "src/components/ui";

export const AdvancedControls = () => {
  const [switchValue, setSwitchValue] = useState(false);

  return (
    <VStack align="stretch" gap={6}>
      <Heading size="lg">Advanced Form Controls</Heading>
      
      {/* Switch */}
      <VStack align="stretch" gap={2}>
        <Text fontSize="sm" fontWeight="semibold">
          Switch
        </Text>
        <HStack gap={4}>
          <Switch
            checked={switchValue}
            onCheckedChange={(event) => setSwitchValue(event.checked)}
          />
          <Text fontSize="sm">Enable notifications</Text>
        </HStack>
      </VStack>

      {/* Additional form controls can be added here as they become available */}
      <VStack align="stretch" gap={2}>
        <Text fontSize="sm" fontWeight="semibold">
          More Controls Coming Soon
        </Text>
        <Text color="fg.muted" fontSize="sm">
          Additional form controls like Select, NumberInput, RadioCard, and FileUpload 
          will be added as their APIs are finalized.
        </Text>
      </VStack>
    </VStack>
  );
};
