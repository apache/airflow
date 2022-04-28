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

import React, { useRef } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
  Button,
  Code,
  Text,
} from '@chakra-ui/react';

import { useContainerRef } from '../../context/containerRef';

const ConfirmDialog = ({
  isOpen, onClose, title = 'Wait a minute', description, body = [], onConfirm, isLoading = false,
}) => {
  const initialFocusRef = useRef();
  const containerRef = useContainerRef();
  return (
    <AlertDialog
      isOpen={isOpen}
      // Since we are not deleting, we can focus on the confirm button
      leastDestructiveRef={initialFocusRef}
      onClose={onClose}
      portalProps={{ containerRef }}
      size="6xl"
      blockScrollOnMount={false}
    >
      <AlertDialogOverlay>
        <AlertDialogContent>
          <AlertDialogHeader fontSize="4xl" fontWeight="bold">
            {title}
          </AlertDialogHeader>

          <AlertDialogBody>
            <Text mb={2}>{description}</Text>
            {Array.isArray(body) && body.map((ti) => (<Code key={ti} fontSize="lg">{ti}</Code>))}
          </AlertDialogBody>

          <AlertDialogFooter>
            <Button onClick={onClose}>
              Cancel
            </Button>
            <Button colorScheme="blue" onClick={onConfirm} ml={3} ref={initialFocusRef} isLoading={isLoading}>
              Confirm
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialogOverlay>
    </AlertDialog>
  );
};

export default ConfirmDialog;
