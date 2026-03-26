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
import { Box, Button, Skeleton, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import { useImportErrorServiceGetImportErrors } from "openapi/queries/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import { StatsCard } from "src/components/StatsCard";

import { DAGImportErrorsModal } from "./DAGImportErrorsModal";

export const DAGImportErrors = ({ iconOnly = false }: { readonly iconOnly?: boolean }) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { i18n, t: translate } = useTranslation("dashboard");

  const isRTL = i18n.dir() === "rtl";

  const { data, error, isLoading } = useImportErrorServiceGetImportErrors({ limit: 1 });
  const importErrorsCount = data?.total_entries ?? 0;

  if (isLoading) {
    // Skeleton dimensions match the rendered component sizes to prevent layout shift.
    // iconOnly: 28px × 60px matches StateBadge (height={7} = 28px in Chakra spacing scale).
    // full: 42px × 175px matches the rendered StatsCard dimensions.
    return iconOnly ? (
      <Skeleton height="28px" width="60px" />
    ) : (
      <Skeleton height="42px" width="175px" />
    );
  }

  if (importErrorsCount === 0 && !error) {
    return undefined;
  }

  return (
    <Box alignItems="center" display="flex">
      <ErrorAlert error={error} />
      {importErrorsCount > 0 &&
        (iconOnly ? (
          <StateBadge
            as={Button}
            colorPalette="failed"
            height={7}
            onClick={onOpen}
            title={translate("importErrors.dagImportError", { count: importErrorsCount })}
          >
            <LuFileWarning size={16} />
            {importErrorsCount}
          </StateBadge>
        ) : (
          <StatsCard
            colorScheme="failed"
            count={importErrorsCount}
            icon={<LuFileWarning />}
            isLoading={isLoading}
            isRTL={isRTL}
            label={translate("importErrors.dagImportError", { count: importErrorsCount })}
            onClick={onOpen}
          />
        ))}
      <DAGImportErrorsModal onClose={onClose} open={open} />
    </Box>
  );
};
