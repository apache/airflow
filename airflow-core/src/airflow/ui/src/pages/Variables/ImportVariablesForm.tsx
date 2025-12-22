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
import { Box, Button, Center, CloseButton, FileUpload, HStack, Spinner } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiUploadCloud } from "react-icons/fi";
import { LuFileUp } from "react-icons/lu";

import type { BulkBody_VariableBody_ } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "src/components/ui/RadioCard";
import { useImportVariables } from "src/queries/useImportVariables";

type ImportVariablesFormProps = {
  readonly onClose: () => void;
};

const actionIfExistsOptions = (translate: TFunction) => [
  {
    description: translate("variables.import.options.fail.description"),
    title: translate("variables.import.options.fail.title"),
    value: "fail",
  },
  {
    description: translate("variables.import.options.overwrite.description"),
    title: translate("variables.import.options.overwrite.title"),
    value: "overwrite",
  },
  {
    description: translate("variables.import.options.skip.description"),
    title: translate("variables.import.options.skip.title"),
    value: "skip",
  },
];

const ImportVariablesForm = ({ onClose }: ImportVariablesFormProps) => {
  const { t: translate } = useTranslation("admin");
  const { error, isPending, mutate, setError } = useImportVariables({
    onSuccessConfirm: onClose,
  });

  const [actionIfExists, setActionIfExists] = useState<"fail" | "overwrite" | "skip">("fail");
  const [isParsing, setIsParsing] = useState(false);
  const [fileContent, setFileContent] = useState<Record<string, unknown> | undefined>(undefined);

  const onFileChange = (file: File) => {
    setIsParsing(true);
    const reader = new FileReader();

    reader.addEventListener("load", (event) => {
      try {
        const text = event.target?.result as string;
        const parsedContent = JSON.parse(text) as Record<string, unknown>;

        setFileContent(parsedContent);
      } catch {
        setError({
          body: {
            detail: translate("variables.import.errorParsingJsonFile"),
          },
        });
        setFileContent(undefined);
      } finally {
        setIsParsing(false);
      }
    });

    reader.readAsText(file);
  };

  const onSubmit = () => {
    setError(undefined);
    if (fileContent) {
      const formattedPayload: BulkBody_VariableBody_ = {
        actions: [
          {
            action: "create" as const,
            action_on_existence: actionIfExists,
            entities: Object.entries(fileContent).map(([key, value]) => ({
              key,
              value,
            })),
          },
        ],
      };

      mutate({ requestBody: formattedPayload });
    }
  };

  return (
    <>
      <FileUpload.Root
        accept={["application/json"]}
        gap="1"
        maxFiles={1}
        mb={6}
        onFileChange={(files) => {
          if (files.acceptedFiles.length > 0) {
            setError(undefined);
            setFileContent(undefined);
            if (files.acceptedFiles[0]) {
              onFileChange(files.acceptedFiles[0]);
            }
          }
        }}
        required
      >
        <FileUpload.HiddenInput />
        <FileUpload.Label fontSize="md" mb={3}>
          {translate("variables.import.upload")}
        </FileUpload.Label>
        <FileUpload.Trigger asChild>
          <Button variant="outline">
            <LuFileUp /> {translate("variables.import.uploadPlaceholder")}
          </Button>
        </FileUpload.Trigger>
        <FileUpload.ItemGroup>
          <FileUpload.Context>
            {({ acceptedFiles }) =>
              acceptedFiles.map((file) => (
                <FileUpload.Item file={file} key={file.name}>
                  <FileUpload.ItemName />
                  <FileUpload.ItemSizeText />
                  <FileUpload.ItemDeleteTrigger
                    asChild
                    onClick={() => {
                      setError(undefined);
                      setFileContent(undefined);
                    }}
                  >
                    <CloseButton size="xs" variant="ghost" />
                  </FileUpload.ItemDeleteTrigger>
                </FileUpload.Item>
              ))
            }
          </FileUpload.Context>
        </FileUpload.ItemGroup>
        {isParsing ? (
          <Center mt={2}>
            <Spinner color="brand.solid" marginRight={2} size="sm" /> Parsing file...
          </Center>
        ) : undefined}
      </FileUpload.Root>
      <RadioCardRoot
        defaultValue="fail"
        mb={6}
        onChange={(event) => {
          const target = event.target as HTMLInputElement;

          setActionIfExists(target.value as "fail" | "overwrite" | "skip");
        }}
      >
        <RadioCardLabel fontSize="md" mb={3}>
          {translate("variables.import.conflictResolution")}
        </RadioCardLabel>
        <HStack align="stretch">
          {actionIfExistsOptions(translate).map((item) => (
            <RadioCardItem
              description={item.description}
              key={item.value}
              label={item.title}
              value={item.value}
            />
          ))}
        </HStack>
      </RadioCardRoot>
      <ErrorAlert error={error} />
      <Box as="footer" display="flex" justifyContent="flex-end" mt={4}>
        {isPending ? (
          <Box bg="bg.muted" inset="0" pos="absolute">
            <Center h="full">
              <Spinner borderWidth="4px" color="brand.solid" size="xl" />
            </Center>
          </Box>
        ) : undefined}
        <Button colorPalette="brand" disabled={!Boolean(fileContent) || isPending} onClick={onSubmit}>
          <FiUploadCloud /> {translate("variables.import.button")}
        </Button>
      </Box>
    </>
  );
};

export default ImportVariablesForm;
