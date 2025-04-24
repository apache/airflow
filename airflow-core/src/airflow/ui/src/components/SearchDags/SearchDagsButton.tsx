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
import { Button, Box, Kbd } from "@chakra-ui/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { MdSearch } from "react-icons/md";

import { Dialog } from "src/components/ui";
import { getMetaKey } from "src/utils";

import { SearchDags } from "./SearchDags";

export const SearchDagsButton = () => {
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
      <Button justifyContent="flex-start" onClick={() => setIsOpen(true)} variant="subtle" w={200}>
        <MdSearch /> Search Dags <Kbd size="sm">{metaKey}+K</Kbd>
      </Button>
      <Dialog.Root onOpenChange={onOpenChange} open={isOpen} size="sm">
        <Dialog.Content>
          <SearchDags setIsOpen={setIsOpen} />
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};
