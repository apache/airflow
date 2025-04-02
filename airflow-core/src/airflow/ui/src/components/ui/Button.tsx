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
import type { ButtonProps as ChakraButtonProps } from "@chakra-ui/react";
import { AbsoluteCenter, Button as ChakraButton, Span, Spinner } from "@chakra-ui/react";
import * as React from "react";

type ButtonLoadingProps = {
  readonly loading?: boolean;
  readonly loadingText?: React.ReactNode;
};

export type ButtonProps = {} & ButtonLoadingProps & ChakraButtonProps;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => {
  const { children, disabled, loading, loadingText, ...rest } = props;

  return (
    <ChakraButton disabled={disabled ?? loading} ref={ref} {...rest}>
      {loading && !Boolean(loadingText) ? (
        <>
          <AbsoluteCenter display="inline-flex">
            <Spinner color="inherit" size="inherit" />
          </AbsoluteCenter>
          <Span opacity={0}>{children}</Span>
        </>
      ) : loading && Boolean(loadingText) ? (
        <>
          <Spinner color="inherit" size="inherit" />
          {loadingText}
        </>
      ) : (
        children
      )}
    </ChakraButton>
  );
});
