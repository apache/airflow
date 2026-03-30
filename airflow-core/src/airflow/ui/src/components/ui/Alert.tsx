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
import { Alert as ChakraAlert, CloseButton } from "@chakra-ui/react";
import { forwardRef } from "react";

export type AlertProps = {
  readonly closable?: boolean;
  readonly endElement?: React.ReactNode;
  readonly icon?: React.ReactElement;
  readonly onClose?: () => void;
  readonly startElement?: React.ReactNode;
  readonly title?: React.ReactNode;
} & Omit<ChakraAlert.RootProps, "title">;

export const Alert = forwardRef<HTMLDivElement, AlertProps>((props, ref) => {
  const { children, closable, endElement, icon, onClose, startElement, title, ...rest } = props;

  return (
    <ChakraAlert.Root ref={ref} {...rest} alignItems="center">
      {startElement ?? <ChakraAlert.Indicator>{icon}</ChakraAlert.Indicator>}
      {Boolean(children) ? (
        <ChakraAlert.Content>
          <ChakraAlert.Title>{title}</ChakraAlert.Title>
          <ChakraAlert.Description>{children}</ChakraAlert.Description>
        </ChakraAlert.Content>
      ) : (
        <ChakraAlert.Title flex="1">{title}</ChakraAlert.Title>
      )}
      {endElement}
      {Boolean(closable) ? (
        <CloseButton
          alignSelf="flex-start"
          insetEnd="-2"
          onClick={onClose}
          pos="relative"
          size="sm"
          top="-2"
        />
      ) : undefined}
    </ChakraAlert.Root>
  );
});
