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
import { Heading, Text, HStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";
import { PiFilePy } from "react-icons/pi";

import type { PluginImportErrorResponse } from "openapi/requests/types.gen";
import { SearchBar } from "src/components/SearchBar";
import { Accordion, Dialog } from "src/components/ui";
import { Pagination } from "src/components/ui/Pagination";

type PluginImportErrorsModalProps = {
  importErrors: Array<PluginImportErrorResponse>;
  onClose: () => void;
  open: boolean;
};

const PAGE_LIMIT = 15;

export const PluginImportErrorsModal: React.FC<PluginImportErrorsModalProps> = ({
  importErrors,
  onClose,
  open,
}) => {
  const { t: translate } = useTranslation("admin");
  const [page, setPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState("");
  const [filteredErrors, setFilteredErrors] = useState(importErrors);

  const startRange = (page - 1) * PAGE_LIMIT;
  const endRange = startRange + PAGE_LIMIT;
  const visibleItems = filteredErrors.slice(startRange, endRange);

  const onOpenChange = () => {
    setSearchQuery("");
    setPage(1);
    onClose();
  };

  useEffect(() => {
    const query = searchQuery.toLowerCase();
    const filtered = importErrors.filter((error) => error.source.toLowerCase().includes(query));

    setFilteredErrors(filtered);
    setPage(1);
  }, [searchQuery, importErrors]);

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open} scrollBehavior="inside" size="xl">
      <Dialog.Content backdrop p={4}>
        <Dialog.Header display="flex" justifyContent="space-between">
          <HStack fontSize="xl">
            <LuFileWarning />
            <Heading>{translate("plugins.importError_one")}</Heading>
          </HStack>
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={searchQuery}
            hideAdvanced
            onChange={setSearchQuery}
            placeHolder={translate("plugins.searchPlaceholder")}
          />
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          <Accordion.Root collapsible multiple size="md" variant="enclosed">
            {visibleItems.map((importError) => (
              <Accordion.Item key={importError.error} value={importError.source}>
                <Accordion.ItemTrigger cursor="pointer">
                  <PiFilePy />
                  {importError.source}
                </Accordion.ItemTrigger>
                <Accordion.ItemContent>
                  <Text color="fg.error" fontSize="sm" whiteSpace="pre-wrap">
                    <code>{importError.error}</code>
                  </Text>
                </Accordion.ItemContent>
              </Accordion.Item>
            ))}
          </Accordion.Root>
        </Dialog.Body>

        <Pagination.Root
          count={filteredErrors.length}
          onPageChange={(event) => setPage(event.page)}
          p={4}
          page={page}
          pageSize={PAGE_LIMIT}
        >
          <HStack>
            <Pagination.PrevTrigger />
            <Pagination.Items />
            <Pagination.NextTrigger />
          </HStack>
        </Pagination.Root>
      </Dialog.Content>
    </Dialog.Root>
  );
};
