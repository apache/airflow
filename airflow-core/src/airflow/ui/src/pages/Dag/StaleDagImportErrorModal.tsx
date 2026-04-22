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
import { Heading, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import type { ImportErrorResponse } from "openapi/requests/types.gen";
import { DagImportErrorAccordion } from "src/components/DagImportErrorAccordion";
import { Dialog } from "src/components/ui";

type Props = {
  readonly importError: ImportErrorResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

export const StaleDagImportErrorModal = ({ importError, onClose, open }: Props) => {
  const { t: translate } = useTranslation("dashboard");
  const itemValue = String(importError.import_error_id);

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} scrollBehavior="inside" size="lg">
      <Dialog.Content backdrop p={4}>
        <Dialog.Header>
          <HStack fontSize="xl" gap={2}>
            <LuFileWarning />
            <Heading>{translate("importErrors.dagImportError", { count: 1 })}</Heading>
          </HStack>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <DagImportErrorAccordion defaultValue={[itemValue]} importErrors={[importError]} />
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
