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
import {
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  type ModalProps,
  Box,
} from "@chakra-ui/react";
import type { ReactNode } from "react";

type Props = {
  readonly header: ReactNode | string;
  readonly subheader?: ReactNode | string;
  readonly submitButton: ReactNode;
} & ModalProps;

const ActionModal = ({
  children,
  header,
  isOpen,
  onClose,
  subheader,
  submitButton,
  ...otherProps
}: Props) => (
  <Modal isOpen={isOpen} onClose={onClose} size="6xl" {...otherProps}>
    <ModalOverlay />
    <ModalContent>
      <ModalHeader>{header}</ModalHeader>
      <ModalCloseButton />
      <ModalBody>
        <Box mb={3}>{subheader}</Box>
        <Box>{children}</Box>
      </ModalBody>
      <ModalFooter justifyContent="space-between">
        <Button colorScheme="gray" onClick={onClose}>
          Cancel
        </Button>
        {submitButton}
      </ModalFooter>
    </ModalContent>
  </Modal>
);

export default ActionModal;
