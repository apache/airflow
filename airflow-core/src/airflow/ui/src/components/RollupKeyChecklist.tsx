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
import { Button, HStack, Text, VStack } from "@chakra-ui/react";
import { FiCheck, FiMinus } from "react-icons/fi";

import { Popover } from "src/components/ui";

type ChecklistProps = {
  readonly receivedKeys: ReadonlyArray<string>;
  readonly requiredKeys: ReadonlyArray<string>;
};

/**
 * Body of the rollup-key checklist: one row per required upstream key, with a
 * tick / dash icon for received / pending. Use this directly when the caller
 * already owns the outer `Popover.Body` (e.g. nested inside another popover).
 */
export const RollupKeyChecklist = ({ receivedKeys, requiredKeys }: ChecklistProps) => {
  const receivedKeySet = new Set(receivedKeys);

  return (
    <VStack align="start" gap={1} maxH="300px" overflowY="auto">
      {requiredKeys.map((key) => {
        const isReceived = receivedKeySet.has(key);

        return (
          <HStack gap={2} key={key}>
            {isReceived ? (
              <FiCheck color="var(--chakra-colors-success-fg)" />
            ) : (
              <FiMinus color="var(--chakra-colors-fg-muted)" />
            )}
            <Text color={isReceived ? "fg.muted" : "fg.default"} fontSize="sm">
              {key}
            </Text>
          </HStack>
        );
      })}
    </VStack>
  );
};

type PopoverProps = {
  readonly isLoading?: boolean;
  readonly receivedCount: number;
  readonly requiredCount: number;
} & ChecklistProps;

/**
 * Standalone popover variant: renders a `count / total` button trigger and shows
 * the {@link RollupKeyChecklist} body in the popover.
 */
export const RollupKeyChecklistPopover = ({
  isLoading,
  receivedCount,
  receivedKeys,
  requiredCount,
  requiredKeys,
}: PopoverProps) => (
  // eslint-disable-next-line jsx-a11y/no-autofocus
  <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
    <Popover.Trigger asChild>
      <Button
        color={receivedCount < requiredCount ? "warning.fg" : "fg.muted"}
        loading={isLoading}
        paddingInline={0}
        size="sm"
        variant="ghost"
      >
        {receivedCount} / {requiredCount}
      </Button>
    </Popover.Trigger>
    <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
      <Popover.Arrow />
      <Popover.Body>
        <RollupKeyChecklist receivedKeys={receivedKeys} requiredKeys={requiredKeys} />
      </Popover.Body>
    </Popover.Content>
  </Popover.Root>
);
