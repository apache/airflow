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

import type { DAGResponse } from "openapi/requests/types.gen";
import { Dialog } from "src/components/ui";

import { Graph } from "../pages/DagsList/Dag/Graph";

type TriggerDAGModalProps = {
  dagDisplayName?: DAGResponse["dag_display_name"];
  dagId?: DAGResponse["dag_id"];
  onClose: () => void;
  open: boolean;
};

export const DagVizModal: React.FC<TriggerDAGModalProps> = ({
  dagDisplayName,
  dagId,
  onClose,
  open,
}) => (
  <Dialog.Root onOpenChange={onClose} open={open} size="full">
    <Dialog.Content backdrop>
      <Dialog.Header bg="blue.muted">
        <Heading size="xl">
          {Boolean(dagDisplayName) ? dagDisplayName : "Dag Undefined"}
        </Heading>
        <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
      </Dialog.Header>
      <Dialog.Body display="flex">
        {dagId === undefined ? undefined : <Graph dagId={dagId} />}
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);
