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
import { FiTrash } from "react-icons/fi";

import ActionButton from "src/components/ui/ActionButton";
import { useDeleteVariable } from "src/queries/useDeleteVariable";

import DeleteVariableDialog from "./DeleteVariableDialog";

type Props = {
  readonly variableKey: string;
};

const DeleteVariableButton = ({ variableKey }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { isPending, mutate } = useDeleteVariable({
    onSuccessConfirm: onClose,
  });

  return (
    <Box>
      <ActionButton
        actionName="Delete Variable"
        icon={<FiTrash />}
        onClick={() => {
          onOpen();
        }}
        text="Delete Variable"
        withText={false}
      />

      <DeleteVariableDialog
        isPending={isPending}
        mutate={mutate}
        onClose={onClose}
        open={open}
        variableKey={variableKey}
      />
    </Box>
  );
};

export default DeleteVariableButton;
