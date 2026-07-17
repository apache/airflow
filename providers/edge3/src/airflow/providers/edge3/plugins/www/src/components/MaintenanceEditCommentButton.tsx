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
import { Button, CloseButton, Dialog, IconButton, Portal, Textarea, useDisclosure } from "@chakra-ui/react";
import { useUiServiceUpdateWorkerMaintenance } from "openapi/queries";
import { useState } from "react";
import { FiEdit } from "react-icons/fi";

interface MaintenanceEditCommentButtonProps {
  onEditComment: (toast: Record<string, string>) => void;
  workerName: string;
}

export const MaintenanceEditCommentButton = ({
  onEditComment,
  workerName,
}: MaintenanceEditCommentButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const [comment, setComment] = useState("");

  const editCommentMutation = useUiServiceUpdateWorkerMaintenance({
    onError: (error) => {
      onEditComment({
        description: `Unable to update comments for worker ${workerName}: ${error}`,
        title: "Updating Comments failed",
        type: "error",
      });
    },
    onSuccess: () => {
      onEditComment({
        description: `Worker maintenance comments for ${workerName} were updated.`,
        title: "Maintenance Comments updated",
        type: "success",
      });
      onClose();
    },
  });

  const editComment = () => {
    editCommentMutation.mutate({ requestBody: { maintenance_comment: comment }, workerName });
  };

  return (
    <>
      <IconButton
        size="sm"
        variant="ghost"
        aria-label="Edit Comments"
        title="Edit Comments"
        onClick={onOpen}
        colorPalette="warning"
      >
        <FiEdit />
      </IconButton>

      <Dialog.Root onOpenChange={onClose} open={open} size="md">
        <Portal>
          <Dialog.Backdrop />
          <Dialog.Positioner>
            <Dialog.Content>
              <Dialog.Header>
                <Dialog.Title>Edit maintenance comments for worker {workerName}</Dialog.Title>
              </Dialog.Header>
              <Dialog.Body>
                <Textarea
                  placeholder="Change maintenance comment (required)"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  required
                  maxLength={1024}
                  size="sm"
                />
              </Dialog.Body>
              <Dialog.Footer>
                <Dialog.ActionTrigger asChild>
                  <Button variant="outline">Cancel</Button>
                </Dialog.ActionTrigger>
                <Button onClick={editComment} disabled={!comment.trim()}>
                  Update Comments
                </Button>
              </Dialog.Footer>
              <Dialog.CloseTrigger asChild>
                <CloseButton size="sm" />
              </Dialog.CloseTrigger>
            </Dialog.Content>
          </Dialog.Positioner>
        </Portal>
      </Dialog.Root>
    </>
  );
};
