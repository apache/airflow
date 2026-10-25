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
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import type { ImportErrorResponse } from "openapi/requests/types.gen";

import { DagImportErrorDetails } from "src/components/DagImportErrorDetails";
import { ErrorModal } from "src/components/ErrorModal";

type Props = {
  readonly importError: ImportErrorResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

export const DagImportErrorModal = ({ importError, onClose, open }: Props) => {
  const { t: translate } = useTranslation("dashboard");

  return (
    <ErrorModal
      icon={<LuFileWarning />}
      onClose={onClose}
      open={open}
      title={translate("importErrors.dagImportError", { count: 1 })}
    >
      <DagImportErrorDetails importError={importError} />
    </ErrorModal>
  );
};
