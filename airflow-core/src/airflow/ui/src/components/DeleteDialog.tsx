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
import { Button, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";

import { Modal } from "src/components/ui";

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

const DeleteDialog = ({
  deleteButtonText,
  isDeleting,
  onClose,
  onDelete,
  open,
  resourceName,
  title,
  warningText,
}: DeleteDialogProps) => {
  const { t: translate } = useTranslation();

  return (
    <Modal
      cancelActionProps={{ "data-testid": "delete-cancel-button" }}
      data-testid="delete-dialog"
      footerActions={
        <Button
          colorPalette="danger"
          data-testid="delete-confirm-button"
          loading={isDeleting}
          onClick={onDelete}
        >
          <FiTrash2 />
          {deleteButtonText ?? translate("modal.delete.button")}
        </Button>
      }
      onOpenChange={onClose}
      open={open}
      title={title}
    >
      <Text>{translate("modal.delete.confirmation", { resourceName })}</Text>
      <Text color="fg.error" fontWeight="bold" mt={4}>
        {warningText}
      </Text>
    </Modal>
  );
};

export default DeleteDialog;
