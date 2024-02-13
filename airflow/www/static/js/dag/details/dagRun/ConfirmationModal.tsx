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

import React, { ReactNode, useRef, cloneElement, ReactElement } from "react";
import {
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  ModalProps,
  Box,
  Checkbox,
} from "@chakra-ui/react";

import { useContainerRef } from "src/context/containerRef";

interface Props extends ModalProps {
  header: ReactNode | string;
  children: ReactNode | string;
  submitButton: ReactElement;
  doNotShowAgain: boolean;
  onDoNotShowAgainChange?: (value: boolean) => void;
}

const ConfirmationModal = ({
  isOpen,
  onClose,
  header,
  children,
  submitButton,
  doNotShowAgain,
  onDoNotShowAgainChange,
  ...otherProps
}: Props) => {
  const containerRef = useContainerRef();
  const submitButtonFocusRef = useRef<HTMLButtonElement>(null);

  const handleClose = () => {
    onClose();
  };

  return (
    <Modal
      size="6xl"
      isOpen={isOpen}
      onClose={handleClose}
      portalProps={{ containerRef }}
      blockScrollOnMount={false}
      initialFocusRef={submitButtonFocusRef}
      {...otherProps}
    >
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>{header}</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Box mb={3}>{children}</Box>
          <Checkbox
            mt={4}
            isChecked={doNotShowAgain}
            onChange={() =>
              onDoNotShowAgainChange && onDoNotShowAgainChange(!doNotShowAgain)
            }
          >
            Do not show this again.
          </Checkbox>
        </ModalBody>
        <ModalFooter justifyContent="space-between">
          <Button colorScheme="gray" onClick={handleClose}>
            Cancel
          </Button>
          {cloneElement(submitButton, { ref: submitButtonFocusRef })}
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default ConfirmationModal;
