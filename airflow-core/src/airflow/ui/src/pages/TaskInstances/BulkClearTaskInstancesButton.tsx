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
import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import ClearTaskInstanceDialog from "src/components/Clear/TaskInstance/ClearTaskInstanceDialog";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly selectedTaskInstances: Array<TaskInstanceResponse>;
};

const BulkClearTaskInstancesButton = ({ clearSelections, selectedTaskInstances }: Props) => {
  const { t: translate } = useTranslation();
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <>
      <Button onClick={onOpen} variant="outline">
        <CgRedo />
        {translate("dags:runAndTaskActions.clear.button", { type: translate("taskInstance_other") })}
      </Button>
      <ClearTaskInstanceDialog
        onCleared={clearSelections}
        onClose={onClose}
        open={open}
        taskInstances={selectedTaskInstances}
      />
    </>
  );
};

export default BulkClearTaskInstancesButton;
