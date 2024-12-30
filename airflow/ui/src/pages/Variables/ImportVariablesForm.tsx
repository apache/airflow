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
import { Box, HStack } from "@chakra-ui/react";
import { useState } from "react";
import { FiUploadCloud } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Button } from "src/components/ui";
import { FileUpload } from "src/components/ui/FileUpload";
import { useImportVariables } from "src/queries/useImportVariables";

type ImportVariableFormProps = {
  readonly onClose: () => void;
};

const ImportVariablesForm = ({ onClose }: ImportVariableFormProps) => {
  const { error, isPending, mutate, setError } = useImportVariables({
    onSuccessConfirm: onClose,
  });

  const [selectedFile, setSelectedFile] = useState<Blob | File | undefined>(
    undefined,
  );

  return (
    <>
      <HStack mb={4}>
        <FileUpload.Root
          accept={["application/json"]}
          alignItems="stretch"
          maxFiles={1}
          maxW="xl"
          onFileChange={(files) => {
            if (files.acceptedFiles.length > 0) {
              setSelectedFile(files.acceptedFiles[0]);
            }
          }}
        >
          <FileUpload.Dropzone
            description="JSON Files accepted"
            label="Drag and drop here to upload"
          />
        </FileUpload.Root>
      </HStack>
      <ErrorAlert error={error} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        <Button
          colorPalette="blue"
          disabled={!Boolean(selectedFile)}
          loading={isPending}
          onClick={() => {
            setError(undefined);
            if (selectedFile) {
              const formData = new FormData();

              formData.append("file", selectedFile);
              mutate({
                actionIfExists: undefined,
                formData: {
                  file: selectedFile,
                },
              });
            }
          }}
        >
          <FiUploadCloud /> Import
        </Button>
      </Box>
    </>
  );
};

export default ImportVariablesForm;
