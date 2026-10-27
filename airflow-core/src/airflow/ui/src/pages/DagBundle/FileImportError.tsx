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
import { Heading, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import { useImportErrorServiceGetImportErrors } from "openapi/queries";

import { Modal } from "src/system-components";

import { DagImportErrorModal } from "src/components/DagImportErrorModal";

type Props = {
  readonly bundleName: string;
  readonly onClose: () => void;
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
export const FileImportError = ({ bundleName, onClose, relativeFileloc }: Props) => {
  const { t: translate } = useTranslation("browse");

  const { data, isLoading } = useImportErrorServiceGetImportErrors(
    { bundleName, filename: relativeFileloc ?? "" },
    undefined,
    { enabled: relativeFileloc !== undefined },
  );

  const [importError] = data?.import_errors ?? [];

  if (importError !== undefined) {
    return (
      <DagImportErrorModal importError={importError} onClose={onClose} open={relativeFileloc !== undefined} />
    );
  }

  // The table polls, so the error can be gone by the time its badge is clicked. Say so rather
  // than leaving the click with no visible effect.
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
      onOpenChange={onClose}
      open={relativeFileloc !== undefined && !isLoading}
    >
      <Text color="fg.muted">{translate("dagBundles.files.importErrorGone")}</Text>
    </Modal>
  );
};
