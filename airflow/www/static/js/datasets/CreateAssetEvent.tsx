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

import React, { useState } from "react";
import {
  Button,
  FormControl,
  FormErrorMessage,
  FormLabel,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Textarea,
} from "@chakra-ui/react";

import { useContainerRef } from "src/context/containerRef";
import { useCreateAssetEvent, useDataset } from "src/api";

interface Props {
  isOpen: boolean;
  onClose: () => void;
  uri: string;
}

function checkJsonString(str: string) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}

const CreateAssetEventModal = ({ uri, isOpen, onClose }: Props) => {
  const containerRef = useContainerRef();
  const [extra, setExtra] = useState("");
  const { data: dataset } = useDataset({ uri });

  const isJson = checkJsonString(extra);
  const isDisabled = !!extra && !isJson;

  const { mutate: createAssetEvent, isLoading } = useCreateAssetEvent({
    datasetId: dataset?.id,
    uri: dataset?.uri,
  });

  const onSubmit = () => {
    createAssetEvent(extra ? JSON.parse(extra) : undefined);
    onClose();
  };

  if (!dataset) return null;

  return (
    <Modal
      size="xl"
      isOpen={isOpen}
      onClose={onClose}
      portalProps={{ containerRef }}
    >
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Manually create event for {dataset?.uri}</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <FormControl isInvalid={isDisabled}>
            <FormLabel>Extra (optional)</FormLabel>
            <Textarea
              value={extra}
              onChange={(e) => setExtra(e.target.value)}
            />
            <FormErrorMessage>Extra needs to be valid JSON</FormErrorMessage>
          </FormControl>
        </ModalBody>
        <ModalFooter justifyContent="space-between">
          <Button colorScheme="gray" onClick={onClose}>
            Cancel
          </Button>
          <Button
            colorScheme="blue"
            disabled={isDisabled}
            onClick={onSubmit}
            isLoading={isLoading}
          >
            Create
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default CreateAssetEventModal;
