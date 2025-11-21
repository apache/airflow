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
import { Text } from "@chakra-ui/react";
import React from "react";
import { useTranslation } from "react-i18next";

import { ConfirmationModal } from "src/components/ConfirmationModal";
import { getRedirectPath } from "src/utils/links.ts";

type LogoutModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

const LogoutModal: React.FC<LogoutModalProps> = ({ isOpen, onClose }) => {
  const { t: translate } = useTranslation("common");

  return (
    <ConfirmationModal
      header={translate("logout")}
      onConfirm={() => {
        const logoutPath = getRedirectPath("api/v2/auth/logout");

        globalThis.location.replace(logoutPath);
      }}
      onOpenChange={onClose}
      open={isOpen}
    >
      <Text>{translate("logoutConfirmation")}</Text>
    </ConfirmationModal>
  );
};

export default LogoutModal;
