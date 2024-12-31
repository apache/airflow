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
import {
  FileUpload as ChakraFileUpload,
  useFileUploadContext,
} from "@chakra-ui/react";
import { forwardRef } from "react";

import { Item } from "./Item";

type VisibilityProps = {
  clearable?: boolean;
  showSize?: boolean;
};

type FileUploadListProps = {
  files?: Array<File>;
} & ChakraFileUpload.ItemGroupProps &
  VisibilityProps;

export const List = forwardRef<HTMLUListElement, FileUploadListProps>(
  (props, ref) => {
    const { clearable, files, showSize, ...rest } = props;

    const fileUpload = useFileUploadContext();
    const acceptedFiles = files ?? fileUpload.acceptedFiles;

    if (acceptedFiles.length === 0) {
      return undefined;
    }

    return (
      <ChakraFileUpload.ItemGroup ref={ref} {...rest}>
        {acceptedFiles.map((file) => (
          <Item
            clearable={clearable}
            file={file}
            key={file.name}
            showSize={showSize}
          />
        ))}
      </ChakraFileUpload.ItemGroup>
    );
  },
);
