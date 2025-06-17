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
import React from "react";
import { useTranslation } from "react-i18next";

import { Dialog } from "src/components/ui";

import LanguageSelector from "./LanguageSelector";

type LanguageModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

const LanguageModal: React.FC<LanguageModalProps> = ({ isOpen, onClose }) => {
  const { t: translate } = useTranslation();

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={isOpen} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>{translate("selectLanguage")}</Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <LanguageSelector />
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default LanguageModal;
