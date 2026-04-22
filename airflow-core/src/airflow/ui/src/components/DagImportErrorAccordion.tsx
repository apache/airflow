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
import { Box, ClipboardRoot, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";
import { PiFilePy } from "react-icons/pi";

import type { ImportErrorResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Accordion, ClipboardIconButton } from "src/components/ui";

type Props = {
  readonly defaultValue?: Array<string>;
  readonly importErrors: ReadonlyArray<ImportErrorResponse>;
  readonly showFileErrorIndicator?: boolean;
};

export const DagImportErrorAccordion = ({
  defaultValue,
  importErrors,
  showFileErrorIndicator = false,
}: Props) => {
  const { t: translate } = useTranslation(["dashboard", "components"]);

  if (importErrors.length === 0) {
    return undefined;
  }

  return (
    <Accordion.Root collapsible defaultValue={defaultValue} multiple size="md" variant="enclosed">
      {importErrors.map((importError) => (
        <Accordion.Item key={importError.import_error_id} value={String(importError.import_error_id)}>
          <HStack align="stretch" gap={0} w="100%">
            <Accordion.ItemTrigger cursor="pointer" flex="1">
              <HStack alignItems="center" flexWrap="wrap" gap={2} w="100%">
                {showFileErrorIndicator ? (
                  <StateBadge borderRadius="full" colorPalette="failed" fontSize="lg" py={1}>
                    <LuFileWarning />
                  </StateBadge>
                ) : undefined}
                <Text display="flex" fontWeight="bold">
                  {translate("components:versionDetails.bundleName")}
                  {": "}
                  {importError.bundle_name}
                </Text>
                <PiFilePy />
                {importError.filename}
              </HStack>
            </Accordion.ItemTrigger>
            <Box alignItems="center" display="flex" flexShrink={0} pr={2}>
              <ClipboardRoot value={importError.filename}>
                <ClipboardIconButton variant="outline" />
              </ClipboardRoot>
            </Box>
          </HStack>
          <Accordion.ItemContent>
            <Text color="fg.muted" fontSize="sm" mb={1}>
              {translate("importErrors.timestamp")}
              {": "}
              <Time datetime={importError.timestamp} />
            </Text>
            <Text color="fg.error" fontSize="sm" whiteSpace="pre-wrap">
              <code>{importError.stack_trace}</code>
            </Text>
          </Accordion.ItemContent>
        </Accordion.Item>
      ))}
    </Accordion.Root>
  );
};
