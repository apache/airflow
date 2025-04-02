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
import { FileUpload as ChakraFileUpload, Icon, IconButton } from "@chakra-ui/react";
import { forwardRef } from "react";
import { LuFile, LuX } from "react-icons/lu";

type VisibilityProps = {
  readonly clearable?: boolean;
  readonly showSize?: boolean;
};

type FileUploadItemProps = {
  readonly file: File;
} & VisibilityProps;

export const Item = forwardRef<HTMLLIElement, FileUploadItemProps>((props, ref) => {
  const { clearable, file, showSize } = props;

  return (
    <ChakraFileUpload.Item file={file} ref={ref}>
      <ChakraFileUpload.ItemPreview asChild>
        <Icon color="fg.muted" fontSize="lg">
          <LuFile />
        </Icon>
      </ChakraFileUpload.ItemPreview>

      {showSize ? (
        <ChakraFileUpload.ItemContent>
          <ChakraFileUpload.ItemName />
          <ChakraFileUpload.ItemSizeText />
        </ChakraFileUpload.ItemContent>
      ) : (
        <ChakraFileUpload.ItemName flex="1" />
      )}

      {clearable ? (
        <ChakraFileUpload.ItemDeleteTrigger asChild>
          <IconButton color="fg.muted" size="xs" variant="ghost">
            <LuX />
          </IconButton>
        </ChakraFileUpload.ItemDeleteTrigger>
      ) : undefined}
    </ChakraFileUpload.Item>
  );
});
