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
import { Box } from "@chakra-ui/react";
import { FiUploadCloud } from "react-icons/fi";

import { Button } from "src/components/ui";
import { FileUpload } from "src/components/ui/FileUpload";

export type ImportVariableBody = {
  action_if_exists: "fail" | "overwrite" | "skip" | undefined;
  formData: { file: Blob | File };
};

const ImportVariablesForm = () => (
  <>
    <FileUpload.Root
      accept={["json"]}
      alignItems="stretch"
      maxFiles={1}
      maxW="xl"
    >
      <FileUpload.Dropzone
        description="JSON Files accepted"
        label="Drag and drop here to upload"
      />
      <FileUpload.List />
    </FileUpload.Root>

    <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
      <Button colorPalette="blue">
        <FiUploadCloud /> Import
      </Button>
    </Box>
  </>
);

export default ImportVariablesForm;
