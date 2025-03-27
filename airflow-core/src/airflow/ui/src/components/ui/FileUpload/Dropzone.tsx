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
import { FileUpload as ChakraFileUpload, Icon, Text } from "@chakra-ui/react";
import { forwardRef } from "react";
import { LuUpload } from "react-icons/lu";

export type FileUploadDropzoneProps = {
  readonly description?: React.ReactNode;
  readonly label: React.ReactNode;
} & ChakraFileUpload.DropzoneProps;

export const Dropzone = forwardRef<HTMLInputElement, FileUploadDropzoneProps>((props, ref) => {
  const { children, description, label, ...rest } = props;

  return (
    <ChakraFileUpload.Dropzone ref={ref} {...rest}>
      <Icon color="fg.muted" fontSize="xl">
        <LuUpload />
      </Icon>
      <ChakraFileUpload.DropzoneContent>
        <div>{label}</div>
        {description ?? <Text color="fg.muted">{description}</Text>}
      </ChakraFileUpload.DropzoneContent>
      {children}
    </ChakraFileUpload.Dropzone>
  );
});
