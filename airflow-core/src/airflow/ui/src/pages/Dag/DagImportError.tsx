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
import { Box, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useImportErrorServiceGetImportErrors } from "openapi/queries";
import type { DAGResponse } from "openapi/requests/types.gen";
import { DagImportErrorsIconBadge } from "src/components/DagImportErrorsIconBadge";

import { DagImportErrorModal } from "./DagImportErrorModal";

type Props = {
  readonly dag: Pick<DAGResponse, "bundle_name" | "is_stale" | "relative_fileloc">;
};

export const DagImportError = ({ dag }: Props) => {
  const { t: translate } = useTranslation("dashboard");
  const { onClose, onOpen, open } = useDisclosure();
  const relativeFileloc = dag.relative_fileloc ?? "";
  const shouldFetch = dag.is_stale && relativeFileloc.length > 0;

  const { data } = useImportErrorServiceGetImportErrors(
    {
      filename: relativeFileloc,
    },
    undefined,
    { enabled: shouldFetch },
  );

  const importError = data?.import_errors[0];

  if (!shouldFetch || !importError) {
    return undefined;
  }

  const importErrorLabel = translate("importErrors.dagImportError", { count: 1 });

  return (
    <Box flexShrink={0}>
      <DagImportErrorsIconBadge
        count={1}
        data-testid="dag-import-error"
        onClick={onOpen}
        title={importErrorLabel}
      />
      <DagImportErrorModal importError={importError} onClose={onClose} open={open} />
    </Box>
  );
};
