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
import { useEffect, useState } from "react";

import { Heading, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import { useImportErrorServiceGetImportErrors } from "openapi/queries";

import { Modal } from "src/system-components";

import { DagImportErrorModal } from "src/components/DagImportErrorModal";
import { ErrorAlert } from "src/components/ErrorAlert";

import { getErrorStatus } from "src/utils/errorHandling";

type Props = {
  readonly bundleName: string;
  readonly hasImportError: boolean;
  readonly onClose: () => void;
  readonly recheckImportError: () => Promise<unknown>;
  readonly relativeFileloc: string | undefined;
};

/**
 * Fetches the import error behind a file's count and hands it to the shared modal.
 *
 * Read through ``GET /importErrors`` rather than returned with the file list: a stack trace is
 * unbounded and the file list is polled, and that endpoint already decides who may read an import
 * error. A dialog rather than a page of its own, because a single stack trace is not worth an
 * addressable route.
 */
export const FileImportError = ({
  bundleName,
  hasImportError,
  onClose,
  recheckImportError,
  relativeFileloc,
}: Props) => {
  const { t: translate } = useTranslation("browse");
  const [recheckedFileloc, setRecheckedFileloc] = useState<string | undefined>(undefined);

  const { data, error, isLoading, refetch } = useImportErrorServiceGetImportErrors(
    { bundleName, filename: relativeFileloc ?? "" },
    undefined,
    { enabled: relativeFileloc !== undefined },
  );

  const [importError] = data?.import_errors ?? [];

  // ``GET /importErrors`` leaves out errors the caller may not read, so an empty result is told
  // apart by the file list, whose count is not filtered by caller. The badge was clicked on a
  // count that may predate a fix, and the empty result may be cached from before the error came
  // back, so both are refetched once before the count is trusted.
  const needsRecheck =
    relativeFileloc !== undefined &&
    data !== undefined &&
    importError === undefined &&
    hasImportError &&
    recheckedFileloc !== relativeFileloc;

  useEffect(() => {
    if (needsRecheck) {
      void Promise.all([recheckImportError(), refetch()]).finally(() => {
        setRecheckedFileloc(relativeFileloc);
      });
    }
  }, [needsRecheck, recheckImportError, refetch, relativeFileloc]);

  const close = () => {
    setRecheckedFileloc(undefined);
    onClose();
  };

  if (importError !== undefined) {
    return (
      <DagImportErrorModal importError={importError} onClose={close} open={relativeFileloc !== undefined} />
    );
  }

  const isForbidden = getErrorStatus(error) === 403 || (data !== undefined && hasImportError);

  return (
    <Modal
      headerProps={{
        children: (
          <HStack gap={2}>
            <LuFileWarning />
            <Heading fontSize="lg">{relativeFileloc}</Heading>
          </HStack>
        ),
      }}
      onOpenChange={close}
      open={relativeFileloc !== undefined && !isLoading && !needsRecheck}
    >
      {error !== null && !isForbidden ? (
        <ErrorAlert error={error} />
      ) : (
        <Text color="fg.muted">
          {translate(
            isForbidden ? "dagBundles.files.importErrorForbidden" : "dagBundles.files.importErrorGone",
          )}
        </Text>
      )}
    </Modal>
  );
};
