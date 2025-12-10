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
import { Box, Button, Dialog, Kbd } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { MdSearch } from "react-icons/md";

import { getMetaKey } from "src/utils";

import { SearchDags } from "./SearchDags";

export const SearchDagsButton = () => {
  const { t: translate } = useTranslation("dags");
  const [isOpen, setIsOpen] = useState(false);
  const metaKey = getMetaKey();

  const onOpenChange = () => {
    setIsOpen(false);
  };

  useHotkeys(
    "mod+k",
    () => {
      setIsOpen(true);
    },
    [isOpen],
    { preventDefault: true },
  );

  return (
    <Box>
      <Button
        colorPalette="brand"
        justifyContent="flex-start"
        minWidth={200}
        onClick={() => setIsOpen(true)}
        variant="subtle"
      >
        <MdSearch /> {translate("search.dags")}{" "}
        <Kbd size="sm">
          {metaKey}
          {translate("search.hotkey")}
        </Kbd>
      </Button>
      <Dialog.Root onOpenChange={onOpenChange} open={isOpen} size="sm">
        <Dialog.Positioner>
          <Dialog.Content>
            <SearchDags setIsOpen={setIsOpen} />
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </Box>
  );
};
