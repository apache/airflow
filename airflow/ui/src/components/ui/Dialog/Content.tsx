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
import { Dialog as ChakraDialog, Portal } from "@chakra-ui/react";
import { forwardRef } from "react";

type ContentProps = {
  backdrop?: boolean;
  portalled?: boolean;
  portalRef?: React.RefObject<HTMLElement>;
} & ChakraDialog.ContentProps;

export const Content = forwardRef<HTMLDivElement, ContentProps>(
  (props, ref) => {
    const {
      backdrop = true,
      children,
      portalled = true,
      portalRef,
      ...rest
    } = props;

    return (
      <Portal container={portalRef} disabled={!portalled}>
        {backdrop ? <ChakraDialog.Backdrop /> : undefined}
        <ChakraDialog.Positioner>
          <ChakraDialog.Content ref={ref} {...rest}>
            {children}
          </ChakraDialog.Content>
        </ChakraDialog.Positioner>
      </Portal>
    );
  },
);
