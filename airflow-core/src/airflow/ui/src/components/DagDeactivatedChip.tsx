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
import { useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";
import { useParams } from "react-router-dom";

import { useDagServiceGetDag, useImportErrorServiceGetImportErrors } from "openapi/queries";

import { DagImportErrorModal } from "./DagImportErrorModal";
import { StatusChip } from "./StatusChip";

export const DagDeactivatedChip = () => {
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
    <>
      <StatusChip
        icon={<LuFileWarning size={14} />}
        label={
          importError === undefined
            ? translate("dag:header.status.deactivated")
            : translate("dashboard:importErrors.dagImportError_one")
        }
        // Inert without a parse error, since there is nothing to open.
        onClick={importError === undefined ? undefined : onOpen}
        severity="error"
      />
      {importError === undefined ? undefined : (
        <DagImportErrorModal importError={importError} onClose={onClose} open={open} />
      )}
    </>
  );
};
