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
import { Tooltip as ChakraTooltip, Portal } from "@chakra-ui/react";
import { forwardRef } from "react";

export type TooltipProps = {
  content: React.ReactNode;
  contentProps?: ChakraTooltip.ContentProps;
  disabled?: boolean;
  portalled?: boolean;
  portalRef?: React.RefObject<HTMLElement>;
  showArrow?: boolean;
} & ChakraTooltip.RootProps;

export const Tooltip = forwardRef<HTMLDivElement, TooltipProps>(
  (props, ref) => {
    const {
      children,
      content,
      contentProps,
      disabled,
      portalled,
      portalRef,
      showArrow,
      ...rest
    } = props;

    if (disabled) {
      return children;
    }

    return (
      <ChakraTooltip.Root closeDelay={100} openDelay={100} {...rest}>
        <ChakraTooltip.Trigger asChild>{children}</ChakraTooltip.Trigger>
        <Portal container={portalRef} disabled={!portalled}>
          <ChakraTooltip.Positioner>
            <ChakraTooltip.Content ref={ref} {...contentProps}>
              {showArrow ? (
                <ChakraTooltip.Arrow>
                  <ChakraTooltip.ArrowTip />
                </ChakraTooltip.Arrow>
              ) : undefined}
              {content}
            </ChakraTooltip.Content>
          </ChakraTooltip.Positioner>
        </Portal>
      </ChakraTooltip.Root>
    );
  },
);
