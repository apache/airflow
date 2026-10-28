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

import {
  useDagServiceGetDag,
  useDagWarningServiceListDagWarnings,
  useImportErrorServiceGetImportErrors,
} from "openapi/queries";

import { DAGWarningsModal } from "src/components/DAGWarningsModal";
import { StatusChip } from "src/components/StatusChip";

import { summarizeDagStatus } from "src/utils";

/**
 * Every problem with a Dag under one marker: a parse failure, the page's own load error, a Dag that
 * has gone from its file, and its parse-time warnings. The worst of them names and colours the chip;
 * the modal lists them all, worst first. Separate chips let one hide behind another and made two
 * identical markers mean different things.
 */
export const DagStatusChip = ({ dagId, error }: { readonly dagId: string; readonly error?: unknown }) => {
  const { t: translate } = useTranslation(["common", "dag", "dashboard", "components"]);
  const { onClose, onOpen, open } = useDisclosure();

  const { data: dag } = useDagServiceGetDag({ dagId }, undefined, { enabled: dagId !== "" });
  const relativeFileloc = dag?.relative_fileloc ?? "";
  const isStale = dag?.is_stale ?? false;

  const { data: importErrorData } = useImportErrorServiceGetImportErrors(
    { bundleName: dag?.bundle_name ?? undefined, filename: relativeFileloc },
    undefined,
    { enabled: isStale && relativeFileloc.length > 0 },
  );
  const { data: warningData } = useDagWarningServiceListDagWarnings({ dagId }, undefined, {
    enabled: dagId !== "",
  });

  const importError = importErrorData?.import_errors[0];
  const warnings = warningData?.dag_warnings ?? [];
  // A Dag that merely vanished from its file is not the same problem as one that failed to parse,
  // so the deactivation only speaks for itself while no parse error explains it.
  const deactivatedMessage =
    isStale && importError === undefined ? translate("dag:header.status.deactivated") : undefined;
  const hasError = importError !== undefined || Boolean(error);

  if (dagId === "" || (!hasError && deactivatedMessage === undefined && warnings.length === 0)) {
    return undefined;
  }

  const { chipLabel, heading, severity } = summarizeDagStatus(translate, {
    deactivatedMessage,
    error,
    importError,
    warnings,
  });

  return (
    <>
      <StatusChip icon={<LuFileWarning />} label={chipLabel} onClick={onOpen} severity={severity} />
      <DAGWarningsModal
        deactivatedMessage={deactivatedMessage}
        error={error}
        heading={heading}
        importError={importError}
        onClose={onClose}
        open={open}
        warnings={warnings}
      />
    </>
  );
};
