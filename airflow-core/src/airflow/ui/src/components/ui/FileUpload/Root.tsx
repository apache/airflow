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
import { FileUpload as ChakraFileUpload } from "@chakra-ui/react";
import { forwardRef } from "react";

export type FileUploadRootProps = {
  readonly inputProps?: React.InputHTMLAttributes<HTMLInputElement>;
} & ChakraFileUpload.RootProps;

export const Root = forwardRef<HTMLInputElement, FileUploadRootProps>((props, ref) => {
  const { children, inputProps, ...rest } = props;

  return (
    <ChakraFileUpload.Root {...rest}>
      <ChakraFileUpload.HiddenInput ref={ref} {...inputProps} />
      {children}
    </ChakraFileUpload.Root>
  );
});
