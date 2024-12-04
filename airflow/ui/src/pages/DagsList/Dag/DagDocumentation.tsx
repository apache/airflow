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
import { Heading } from "@chakra-ui/react";
import Markdown from "react-markdown";

import type { DAGDetailsResponse } from "openapi/requests/types.gen";
import { Dialog } from "src/components/ui";

type DagDocumentationModalProps = {
  docMd: DAGDetailsResponse["doc_md"];
  onClose: () => void;
  open: boolean;
};

export const DagDocumentation: React.FC<DagDocumentationModalProps> = ({
  docMd,
  onClose,
  open,
}) => (
  <Dialog.Root onOpenChange={onClose} open={open} size="md">
    <Dialog.Content backdrop>
      <Dialog.Header bg="blue.muted">
        <Heading size="xl">Dag Documentation</Heading>
        <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
      </Dialog.Header>
      <Dialog.Body display="flex">
        <Markdown>{docMd}</Markdown>
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);
