/* eslint-disable i18next/no-literal-string */

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
import { Dialog, Heading, Text, Button } from "@chakra-ui/react";

type ModalDialogProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

export const ModalDialog = ({ isOpen, onClose }: ModalDialogProps) => (
  <Dialog.Root onOpenChange={() => onClose()} open={isOpen}>
    <Dialog.Content>
      <Dialog.Header>
        <Heading size="xl">Example Modal Dialog</Heading>
        <Dialog.CloseTrigger />
      </Dialog.Header>
      <Dialog.Body>
        <Text>
          This modal demonstrates proper focus management and accessibility features. Modal content goes here
          with proper ARIA attributes, keyboard navigation support, and focus management for accessibility
          testing.
        </Text>
        <Button colorPalette="brand" onClick={onClose}>
          Close Dialog
        </Button>
      </Dialog.Body>
    </Dialog.Content>
  </Dialog.Root>
);
