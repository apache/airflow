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
import { LuFileWarning } from "react-icons/lu";

import type { DAGWarningResponse } from "openapi/requests/types.gen";
import { Dialog } from "src/components/ui";

import { ErrorAlert } from "../ErrorAlert";
import { WarningAlert } from "../WarningAlert";

type ImportDAGErrorModalProps = {
  error?: unknown;
  onClose: () => void;
  open: boolean;
  warnings?: Array<DAGWarningResponse>;
};

export const DAGWarningsModal: React.FC<ImportDAGErrorModalProps> = ({ error, onClose, open, warnings }) => (
  <Dialog.Root onOpenChange={onClose} open={open} scrollBehavior="inside" size="xl">
    <Dialog.Content backdrop>
      <Dialog.Header>
        <HStack fontSize="xl">
          <LuFileWarning />
          <Heading>Dag Warnings / Errors</Heading>
        </HStack>
      </Dialog.Header>

      <Dialog.CloseTrigger />

      <Dialog.Body>
        {Boolean(error) && <ErrorAlert error={error} />}
        {warnings?.map((warning) => <WarningAlert key={warning.message} warning={warning} />)}
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);
