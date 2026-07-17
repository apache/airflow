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
import { Tag as ChakraTag } from "@chakra-ui/react";
import { forwardRef } from "react";

export type TagProps = {
  readonly closable?: boolean;
  readonly endElement?: React.ReactNode;
  readonly onClose?: VoidFunction;
  readonly startElement?: React.ReactNode;
} & ChakraTag.RootProps;

export const Tag = forwardRef<HTMLSpanElement, TagProps>((props, ref) => {
  const { children, onClose, closable = Boolean(onClose), endElement, startElement, ...rest } = props;

  return (
    <ChakraTag.Root ref={ref} {...rest}>
      {Boolean(startElement) ? <ChakraTag.StartElement>{startElement}</ChakraTag.StartElement> : undefined}
      <ChakraTag.Label>{children}</ChakraTag.Label>
      {Boolean(endElement) ? <ChakraTag.EndElement>{endElement}</ChakraTag.EndElement> : undefined}
      {Boolean(closable) ? (
        <ChakraTag.EndElement>
          <ChakraTag.CloseTrigger onClick={onClose} />
        </ChakraTag.EndElement>
      ) : undefined}
    </ChakraTag.Root>
  );
});
