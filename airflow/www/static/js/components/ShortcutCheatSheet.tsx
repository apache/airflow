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

import React, { ReactNode } from "react";
import {
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  TableCaption,
  TableContainer,
  Kbd,
} from "@chakra-ui/react";
import type { KeyboardShortcutIdentifier } from "src/types";
import { useContainerRef } from "src/context/containerRef";

interface Props {
  isOpen: boolean;
  onClose: () => void;
  header: ReactNode | string;
  keyboardShortcutIdentifier: KeyboardShortcutIdentifier;
}

const ShortcutCheatSheet = ({
  isOpen,
  onClose,
  header,
  keyboardShortcutIdentifier,
  ...otherProps
}: Props) => {
  const containerRef = useContainerRef();
  return (
    <Modal
      size="6xl"
      isOpen={isOpen}
      onClose={onClose}
      portalProps={{ containerRef }}
      blockScrollOnMount={false}
      {...otherProps}
    >
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>{header}</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <TableContainer>
            <Table variant="striped">
              <TableCaption>
                Press <Kbd>ESC</Kbd> to close
              </TableCaption>
              <Thead>
                <Tr>
                  <Th>Shortcut Key</Th>
                  <Th>Action</Th>
                </Tr>
              </Thead>
              <Tbody>
                {Object.keys(keyboardShortcutIdentifier).map((item) => (
                  <Tr key={item}>
                    <Td>
                      <Kbd>{keyboardShortcutIdentifier[item].primaryKey}</Kbd> +{" "}
                      <Kbd>
                        {keyboardShortcutIdentifier[item].secondaryKey[0]}
                      </Kbd>
                    </Td>
                    <Td>{keyboardShortcutIdentifier[item].detail}</Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          </TableContainer>
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default ShortcutCheatSheet;
