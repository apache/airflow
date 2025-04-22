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
import { Text, Heading, HStack } from "@chakra-ui/react";
import React from "react";
import { FiTrash2 } from "react-icons/fi";

import { Button, Dialog } from "src/components/ui";

type DeleteDialogProps = {
  readonly deleteButtonText?: string;
  readonly isDeleting: boolean;
  readonly onClose: () => void;
  readonly onDelete: () => void;
  readonly open: boolean;
  readonly resourceName: string;
  readonly title: string;
  readonly warningText: string;
};

const DeleteDialog: React.FC<DeleteDialogProps> = ({
  deleteButtonText = "Delete",
  isDeleting,
  onClose,
  onDelete,
  open,
  resourceName,
  title,
  warningText,
}) => (
  <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="md" unmountOnExit>
    <Dialog.Content backdrop>
      <Dialog.Header>
        <Heading size="lg">{title}</Heading>
      </Dialog.Header>
      <Dialog.CloseTrigger />
      <Dialog.Body>
        <Text>
          Are you sure you want to delete <strong>{resourceName}</strong>? This action cannot be undone.
        </Text>
        <Text color="red.500" fontWeight="bold" mt={4}>
          {warningText}
        </Text>
      </Dialog.Body>
      <Dialog.Footer>
        <HStack justifyContent="flex-end" width="100%">
          <Button onClick={onClose} variant="outline">
            Cancel
          </Button>
          <Button colorPalette="red" loading={isDeleting} onClick={onDelete}>
            <FiTrash2 style={{ marginRight: "8px" }} /> {deleteButtonText}
          </Button>
        </HStack>
      </Dialog.Footer>
    </Dialog.Content>
  </Dialog.Root>
);

export default DeleteDialog;
