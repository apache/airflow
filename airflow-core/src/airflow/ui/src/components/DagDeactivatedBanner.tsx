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
import { Button, HStack, Text, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";
import { useParams } from "react-router-dom";

import { useDagServiceGetDag, useImportErrorServiceGetImportErrors } from "openapi/queries";
import { DagImportErrorModal } from "src/pages/Dag/DagImportErrorModal";

export const DagDeactivatedBanner = () => {
  const { t: translate } = useTranslation(["dag", "dashboard"]);
  const { dagId = "" } = useParams();
  const { onClose, onOpen, open } = useDisclosure();

  const { data: dag } = useDagServiceGetDag({ dagId }, undefined, { enabled: dagId !== "" });
  const relativeFileloc = dag?.relative_fileloc ?? "";
  const bundleName = dag?.bundle_name ?? undefined;

  const { data } = useImportErrorServiceGetImportErrors(
    { bundleName, filename: relativeFileloc },
    undefined,
    { enabled: dag?.is_stale && relativeFileloc.length > 0 },
  );

  if (dagId === "" || !dag?.is_stale) {
    return undefined;
  }

  const importError = data?.import_errors[0];

  return (
    <HStack bg="bg.warning" color="fg.warning" justifyContent="space-between" px={3} py={1}>
      <Text>{translate("header.status.deactivated")}</Text>
      {importError ? (
        <>
          <Button
            borderColor="fg.warning"
            colorPalette="warning"
            onClick={onOpen}
            size="xs"
            variant="outline"
          >
            <HStack gap={1}>
              <LuFileWarning size={14} />
              {translate("dashboard:importErrors.dagImportError", { count: 1 })}
            </HStack>
          </Button>
          <DagImportErrorModal importError={importError} onClose={onClose} open={open} />
        </>
      ) : undefined}
    </HStack>
  );
};
