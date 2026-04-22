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

import { StaleDagImportErrorModal } from "./StaleDagImportErrorModal";
import { selectLatestMatchingImportError } from "./selectLatestMatchingImportError";

const IMPORT_ERROR_FETCH_LIMIT = 100;

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
      filenamePattern: relativeFileloc,
      limit: IMPORT_ERROR_FETCH_LIMIT,
      offset: 0,
      orderBy: ["-timestamp"],
    },
    undefined,
    { enabled: shouldFetch },
  );

  if (!shouldFetch) {
    return undefined;
  }

  const matched = selectLatestMatchingImportError(data?.import_errors, relativeFileloc, dag.bundle_name);

  if (matched === undefined) {
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
      <StaleDagImportErrorModal importError={matched} onClose={onClose} open={open} />
    </Box>
  );
};
