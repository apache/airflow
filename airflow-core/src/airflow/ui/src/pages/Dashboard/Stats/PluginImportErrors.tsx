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
import { Box, Button, useDisclosure, Skeleton } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuPlug } from "react-icons/lu";

import { usePluginServiceImportErrors } from "openapi/queries";
import { ErrorAlert, type ExpandedApiError } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import { StatsCard } from "src/components/StatsCard";

import { PluginImportErrorsModal } from "./PluginImportErrorsModal";

export const PluginImportErrors = ({ iconOnly = false }: { readonly iconOnly?: boolean }) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { i18n, t: translate } = useTranslation("admin");
  const { data, error, isLoading } = usePluginServiceImportErrors();

  const isRTL = i18n.dir() === "rtl";

  const importErrorsCount = data?.total_entries ?? 0;
  const importErrors = data?.import_errors ?? [];

  if (isLoading) {
    return <Skeleton height="9" width="225px" />;
  }

  if (Boolean(error) && (error as ExpandedApiError).status === 403) {
    return undefined;
  }

  if (importErrorsCount === 0) {
    return undefined;
  }

  return (
    <Box alignItems="center" display="flex">
      <ErrorAlert error={error} />
      {iconOnly ? (
        <StateBadge
          as={Button}
          colorPalette="failed"
          height={7}
          onClick={onOpen}
          title={translate("plugins.importError", { count: importErrorsCount })}
        >
          <LuPlug size={8} />
          {importErrorsCount}
        </StateBadge>
      ) : (
        <StatsCard
          colorScheme="failed"
          count={importErrorsCount}
          icon={<LuPlug />}
          isLoading={isLoading}
          isRTL={isRTL}
          label={translate("plugins.importError", { count: importErrorsCount })}
          onClick={onOpen}
        />
      )}
      <PluginImportErrorsModal importErrors={importErrors} onClose={onClose} open={open} />
    </Box>
  );
};
