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

import { ConfirmationModal } from "src/components/ConfirmationModal";
import { TOKEN_STORAGE_KEY } from "src/utils/tokenHandler";

type LogoutModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

const LogoutModal: React.FC<LogoutModalProps> = ({ isOpen, onClose }) => (
  <ConfirmationModal
    header="Logout"
    onConfirm={() => {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
      globalThis.location.replace(`/api/v2/auth/logout`);
    }}
    onOpenChange={onClose}
    open={isOpen}
  >
    <Text>You are about to logout from the application.</Text>
  </ConfirmationModal>
);

export default LogoutModal;
