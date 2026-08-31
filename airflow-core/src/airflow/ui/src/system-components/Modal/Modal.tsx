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
import { Button, type ButtonProps, type Dialog as ChakraDialog, Flex } from "@chakra-ui/react";
import type { ComponentProps, ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { Dialog } from "src/system-components/Dialog";

export type ModalProps = {
  readonly bodyProps?: ChakraDialog.BodyProps;
  readonly cancelActionProps?: { readonly "data-testid"?: string } & ButtonProps;
  readonly children?: ReactNode;
  readonly contentProps?: ComponentProps<typeof Dialog.Content>;
  readonly footerActions?: ReactNode;
  readonly footerProps?: ChakraDialog.FooterProps;
  readonly headerProps?: ChakraDialog.HeaderProps;
  readonly hideCancelAction?: boolean;
  readonly hideCloseButton?: boolean;
  readonly title?: ReactNode;
} & Omit<ChakraDialog.RootProps, "children">;

/**
 * Wraps the Chakra `Dialog` parts so a modal is declared through props rather than
 * assembled from Root/Content/Header/CloseTrigger/Body/Footer at every call site.
 *
 * Each slot accepts a `*Props` escape hatch, and passing `children` through one of
 * those overrides that slot's default content entirely.
 *
 * A dialog that supplies `footerActions` also gets a Cancel action beside them,
 * unless `hideCancelAction` is set. Dialogs with no footer actions are unaffected —
 * a lone Cancel is not reason enough to grow a footer.
 */
export const Modal = ({
  bodyProps,
  cancelActionProps,
  children,
  contentProps,
  footerActions,
  footerProps,
  headerProps,
  hideCancelAction = false,
  hideCloseButton = false,
  lazyMount = true,
  title,
  unmountOnExit = true,
  ...rest
}: ModalProps) => {
  const { t: translate } = useTranslation("common");

  const titleContent = title === undefined ? undefined : <Dialog.Title>{title}</Dialog.Title>;
  const headerContent = headerProps?.children ?? titleContent;
  const actionsContent =
    footerActions === undefined ? undefined : (
      // `row-reverse` renders the first action rightmost, so the primary action is
      // written first and therefore reached first when tabbing.
      <Flex flexDirection="row-reverse" gap={2} w="100%">
        {footerActions}
        {hideCancelAction ? undefined : (
          <Dialog.ActionTrigger asChild>
            <Button variant="outline" {...cancelActionProps}>
              {cancelActionProps?.children ?? translate("modal.cancel")}
            </Button>
          </Dialog.ActionTrigger>
        )}
      </Flex>
    );
  const footerContent = footerProps?.children ?? actionsContent;

  return (
    <Dialog.Root lazyMount={lazyMount} unmountOnExit={unmountOnExit} {...rest}>
      <Dialog.Content {...contentProps}>
        {headerContent === undefined ? undefined : (
          <Dialog.Header
            {...headerProps}
            alignItems="center"
            gap={2}
            justifyContent="space-between"
            paddingRight={hideCloseButton ? undefined : 3}
          >
            {headerContent}
            {hideCloseButton ? undefined : <Dialog.CloseTrigger />}
          </Dialog.Header>
        )}
        <Dialog.Body {...bodyProps}>{bodyProps?.children ?? children}</Dialog.Body>
        {footerContent === undefined ? undefined : (
          <Dialog.Footer {...footerProps}>{footerContent}</Dialog.Footer>
        )}
      </Dialog.Content>
    </Dialog.Root>
  );
};
