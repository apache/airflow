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
import { Box, GridItem, Heading, HStack, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { MdInfoOutline } from "react-icons/md";

import { Stat } from "src/components/Stat";
import { Dialog } from "src/components/ui";

import ActionButton from "./ui/ActionButton";

const StatsDisplay = ({
  isCompact = false,
  stats,
  title,
}: {
  readonly isCompact?: boolean;
  readonly stats: Array<{ label: string; value: React.ReactNode | string }>;
  readonly title?: string;
}) => {
  const [isOpen, setIsOpen] = useState(false);

  if (isCompact) {
    return (
      <Box>
        <ActionButton
          actionName="Show stats"
          icon={<MdInfoOutline />}
          onClick={() => setIsOpen(true)}
          text="Stats"
          withText={false}
        />
        <Dialog.Root lazyMount onOpenChange={() => setIsOpen(false)} open={isOpen} size="md">
          <Dialog.Content backdrop>
            <Dialog.Header bg="blue.muted">
              <Heading size="xl">{title ?? "Statistics"}</Heading>
              <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
            </Dialog.Header>
            <Dialog.Body alignItems="flex-start" as={VStack} gap={3}>
              {stats.map(({ label, value }) => (
                <Box key={label} w="100%">
                  <Stat label={label}>{value}</Stat>
                </Box>
              ))}
            </Dialog.Body>
          </Dialog.Content>
        </Dialog.Root>
      </Box>
    );
  }

  return (
    <HStack alignItems="flex-start" flexWrap="wrap" gap={5} justifyContent="space-between">
      {stats.map(({ label, value }) => (
        <GridItem key={label}>
          <Stat label={label}>{value}</Stat>
        </GridItem>
      ))}
    </HStack>
  );
};

export default StatsDisplay;
