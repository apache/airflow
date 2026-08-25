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
import { Button, type DialogBodyProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Modal } from "src/components/ui";

type Props = {
  readonly children?: DialogBodyProps["children"];
  readonly header: string;
  readonly onConfirm: () => void;
  readonly onOpenChange: (details: { open: boolean }) => void;
  readonly open: boolean;
};

export const ConfirmationModal = ({ children, header, onConfirm, onOpenChange, open }: Props) => {
  const { t: translate } = useTranslation();

  return (
    <Modal
      cancelActionProps={{ "data-testid": "confirmation-cancel-button" }}
      data-testid="confirmation-modal"
      footerActions={
        <Button
          data-testid="confirmation-confirm-button"
          onClick={() => {
            onConfirm();
            onOpenChange({ open });
          }}
        >
          {translate("modal.confirm")}
        </Button>
      }
      onOpenChange={onOpenChange}
      open={open}
      title={header}
    >
      {children}
    </Modal>
  );
};
