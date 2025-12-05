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
import { Box, Popover, Portal, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown } from "react-icons/fi";

import { Button } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";

type Props = {
  readonly setShowDagLevelDependencies: React.Dispatch<React.SetStateAction<boolean>>;
  readonly setShowTaskLevelDependencies: React.Dispatch<React.SetStateAction<boolean>>;
  readonly showDagLevelDependencies: boolean;
  readonly showTaskLevelDependencies: boolean;
};

export const AssetPanelButtons = ({
  setShowDagLevelDependencies,
  setShowTaskLevelDependencies,
  showDagLevelDependencies,
  showTaskLevelDependencies,
}: Props) => {
  const { t: translate } = useTranslation(["assets", "common"]);

  return (
    <Box position="absolute" px={2} right={2} top={1} zIndex={1}>
      {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
      <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
        <Popover.Trigger asChild>
          <Button bg="bg.subtle" color="fg.default" size="sm" variant="outline">
            {translate("common:options")}
            <FiChevronDown size={8} />
          </Button>
        </Popover.Trigger>
        <Portal>
          <Popover.Positioner>
            <Popover.Content w="fit-content">
              <Popover.Arrow />
              <Popover.Body display="flex" flexDirection="column" gap={2} p={2}>
                <VStack alignItems="flex-start" px={1}>
                  <Checkbox
                    checked={showDagLevelDependencies}
                    onChange={() => setShowDagLevelDependencies(!showDagLevelDependencies)}
                    size="sm"
                  >
                    {translate("assets:showDagLevelDependencies")}
                  </Checkbox>
                  <Checkbox
                    checked={showTaskLevelDependencies}
                    onChange={() => setShowTaskLevelDependencies(!showTaskLevelDependencies)}
                    size="sm"
                  >
                    {translate("assets:showTaskLevelDependencies")}
                  </Checkbox>
                </VStack>
              </Popover.Body>
            </Popover.Content>
          </Popover.Positioner>
        </Portal>
      </Popover.Root>
    </Box>
  );
};
