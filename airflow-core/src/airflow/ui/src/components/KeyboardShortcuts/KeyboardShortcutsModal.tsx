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
import { Box, Flex, HStack, Heading, Kbd, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { Dialog } from "src/components/ui";
import {
  SHORTCUT_CATEGORIES,
  SHORTCUTS,
  type ShortcutEntry,
  useShortcutRegistry,
} from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";
import { getMetaKey } from "src/utils";

import { formatShortcutCombo } from "./formatShortcutCombo";

const buildGroups = (shortcuts: ReadonlyArray<ShortcutEntry>) =>
  SHORTCUT_CATEGORIES.map((category) => {
    const seen = new Set<string>();
    const items = shortcuts
      .filter((entry) => entry.category === category)
      .filter((entry) => {
        const key = `${entry.keys.join("+")}|${entry.description}`;

        if (seen.has(key)) {
          return false;
        }
        seen.add(key);

        return true;
      });

    return { category, items };
  }).filter((group) => group.items.length > 0);

export const KeyboardShortcutsModal = () => {
  const { t: translate } = useTranslation("common");
  const { shortcuts } = useShortcutRegistry();
  const [open, setOpen] = useState(false);
  const metaKey = getMetaKey();

  const toggle = () => setOpen((prev) => !prev);

  useShortcut({
    ...SHORTCUTS.global.showHelp,
    callback: toggle,
  });

  const groups = buildGroups(shortcuts);

  return (
    <Dialog.Root lazyMount onOpenChange={(event) => setOpen(event.open)} open={open} size="md">
      <Dialog.Content backdrop>
        <Dialog.Header>{translate("shortcuts.title")}</Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {groups.length === 0 ? (
            <Text color="fg.muted">{translate("shortcuts.empty")}</Text>
          ) : (
            <VStack align="stretch" gap={4}>
              {groups.map(({ category, items }) => (
                <Box key={category}>
                  <Heading color="fg.muted" mb={2} size="xs" textTransform="uppercase">
                    {translate(`shortcuts.categories.${category}`)}
                  </Heading>
                  <VStack align="stretch" gap={1}>
                    {items.map((entry) => (
                      <Flex alignItems="center" gap={4} justifyContent="space-between" key={entry.id}>
                        <Text fontSize="sm">{entry.description}</Text>
                        <HStack gap={1}>
                          {entry.keys.map((combo) => (
                            <Kbd key={combo} size="sm">
                              {formatShortcutCombo(combo, metaKey)}
                            </Kbd>
                          ))}
                        </HStack>
                      </Flex>
                    ))}
                  </VStack>
                </Box>
              ))}
            </VStack>
          )}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
