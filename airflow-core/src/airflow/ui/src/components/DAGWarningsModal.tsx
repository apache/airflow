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
import { Heading, HStack, Spacer, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import type { DAGWarningResponse, ImportErrorResponse } from "openapi/requests/types.gen";

import { Modal } from "src/system-components";

import { getApiErrorDetail } from "src/utils";

import { DagImportErrorDetails } from "./DagImportErrorDetails";
import type { ExpandedApiError } from "./ErrorAlert";
import { StatusMessage } from "./StatusMessage";
import { WarningAlert } from "./WarningAlert";

/** The modal lists the page error as one more item next to the warnings, so the button count does too. */
export const countDagWarnings = (warningsTotal: number | undefined, error: unknown): number =>
  (warningsTotal ?? 0) + Number(Boolean(error));

type ImportDAGErrorModalProps = {
  /** Stated as one more warning, for a Dag that is no longer in its file. */
  readonly deactivatedMessage?: string;
  readonly error?: unknown;
  readonly importError?: ImportErrorResponse;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly warnings?: Array<DAGWarningResponse>;
};

export const DAGWarningsModal = ({
  deactivatedMessage,
  error,
  importError,
  onClose,
  open,
  warnings,
}: ImportDAGErrorModalProps) => {
  const { t: translate } = useTranslation(["components", "common", "dashboard"]);
  const warningCount = (warnings?.length ?? 0) + Number(deactivatedMessage !== undefined);
  // Named for the worst thing in it, since that is what the reader came for.
  const errorHeading =
    importError === undefined
      ? translate("dagWarnings.error_one")
      : translate("dashboard:importErrors.dagImportError", { count: 1 });
  const hasError = Boolean(error) || importError !== undefined;
  const apiError = error as ExpandedApiError | undefined;
  // What the request itself said, falling back to the bare word when it carried no status or message.
  const pageErrorHeading =
    [apiError?.status, apiError?.message].filter(Boolean).join(" ") || translate("common:error.title");
  const detail = getApiErrorDetail(error);
  const errorDetail = Array.isArray(detail) ? detail.join("\n") : detail;
  let heading = "";

  if (hasError) {
    heading =
      warningCount > 0
        ? translate("dagWarnings.errorAndWarning", {
            warningText: translate("dagWarnings.warning", { count: warningCount }),
          })
        : errorHeading;
  } else if (warningCount > 0) {
    heading = translate("dagWarnings.warning", { count: warningCount });
  }

  return (
    <Modal
      data-testid="dag-warnings-modal"
      headerProps={{
        children: (
          <HStack gap={2}>
            <LuFileWarning />
            <Heading fontSize="lg">{heading}</Heading>
          </HStack>
        ),
      }}
      onOpenChange={onClose}
      open={open}
      scrollBehavior="inside"
    >
      {importError === undefined ? undefined : (
        <VStack>
          <StatusMessage
            body={<DagImportErrorDetails importError={importError} />}
            heading={translate("dashboard:importErrors.dagImportError", { count: 1 })}
            severity="error"
          />
          <Spacer />
        </VStack>
      )}
      {Boolean(error) && (
        <VStack>
          <StatusMessage body={errorDetail} heading={pageErrorHeading} severity="error" />
          <Spacer />
        </VStack>
      )}
      {deactivatedMessage === undefined ? undefined : (
        <VStack>
          <StatusMessage heading={deactivatedMessage} severity="warning" />
          <Spacer />
        </VStack>
      )}
      {warnings?.map((warning) => (
        <VStack key={warning.message}>
          <WarningAlert warning={warning} />
          <Spacer />
        </VStack>
      ))}
    </Modal>
  );
};
