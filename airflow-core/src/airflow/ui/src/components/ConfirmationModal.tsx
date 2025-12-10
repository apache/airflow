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
import { Button, CloseButton, Dialog, type DialogBodyProps, Heading } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

type Props = {
  readonly children?: DialogBodyProps["children"];
  readonly header: string;
  readonly onConfirm: () => void;
  readonly onOpenChange: (details: { open: boolean }) => void;
  readonly open: boolean;
};

export const ConfirmationModal = ({ children, header, onConfirm, onOpenChange, open }: Props) => {
  const { t: translate } = useTranslation("common");

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open}>
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content>
          <Dialog.Header>
            <Heading>{header}</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
            <CloseButton size="sm" />
          </Dialog.CloseTrigger>

          <Dialog.Body>{children}</Dialog.Body>
          <Dialog.Footer>
            <Dialog.ActionTrigger asChild>
              <Button onClick={() => onOpenChange({ open })} variant="outline">
                {translate("modal.cancel")}
              </Button>
            </Dialog.ActionTrigger>
            <Button
              colorPalette="brand"
              onClick={() => {
                onConfirm();
                onOpenChange({ open });
              }}
            >
              {translate("modal.confirm")}
            </Button>
          </Dialog.Footer>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};
