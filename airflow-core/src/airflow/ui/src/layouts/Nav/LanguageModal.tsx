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
import { CloseButton, Dialog } from "@chakra-ui/react";
import React from "react";
import { useTranslation } from "react-i18next";

import LanguageSelector from "./LanguageSelector";

type LanguageModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

const LanguageModal: React.FC<LanguageModalProps> = ({ isOpen, onClose }) => {
  const { t: translate } = useTranslation();

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} size="xl">
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content>
          <Dialog.Header>{translate("selectLanguage")}</Dialog.Header>
          <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
            <CloseButton size="sm" />
          </Dialog.CloseTrigger>
          <Dialog.Body>
            <LanguageSelector />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};

export default LanguageModal;
