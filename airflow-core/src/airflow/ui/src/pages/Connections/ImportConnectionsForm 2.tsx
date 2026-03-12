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
import { parse as parseYaml } from "yaml";

import type { BulkBody_ConnectionBody_, ConnectionBody } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { RadioCardItem, RadioCardLabel, RadioCardRoot } from "src/components/ui/RadioCard";
import { useImportConnections } from "src/queries/useImportConnections";

type ImportConnectionsFormProps = {
  readonly onClose: () => void;
};

type ConnectionExportEntry = {
  conn_type: string;
  description?: string | null;
  extra?: string | null;
  host?: string | null;
  login?: string | null;
  password?: string | null;
  port?: number | null;
  schema?: string | null;
};

function parseConnectionsFile(content: string): Record<string, ConnectionExportEntry> {
  const trimmed = content.trim();

  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    const parsed = JSON.parse(content) as Record<string, ConnectionExportEntry>;

    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new Error("Invalid JSON structure");
    }
    return parsed;
  }

  const parsed = parseYaml(content) as Record<string, ConnectionExportEntry>;

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Invalid YAML structure");
  }
  return parsed;
}

function toConnectionBody(connId: string, entry: ConnectionExportEntry): ConnectionBody {
  const extra = entry.extra;
  const extraStr =
    extra === undefined || extra === null
      ? undefined
      : typeof extra === "string"
        ? extra
        : JSON.stringify(extra);

  return {
    connection_id: connId,
    conn_type: entry.conn_type,
    description: entry.description ?? undefined,
    extra: extraStr ?? undefined,
    host: entry.host ?? undefined,
    login: entry.login ?? undefined,
    password: entry.password ?? undefined,
    port: entry.port ?? undefined,
    schema: entry.schema ?? undefined,
    team_name: undefined,
  };
}

const actionIfExistsOptions = (translate: TFunction) => [
  {
    description: translate("connections.import.options.fail.description"),
    title: translate("connections.import.options.fail.title"),
    value: "fail",
  },
  {
    description: translate("connections.import.options.overwrite.description"),
    title: translate("connections.import.options.overwrite.title"),
    value: "overwrite",
  },
  {
    description: translate("connections.import.options.skip.description"),
    title: translate("connections.import.options.skip.title"),
    value: "skip",
  },
];

const ImportConnectionsForm = ({ onClose }: ImportConnectionsFormProps) => {
  const { t: translate } = useTranslation("admin");
  const { error, isPending, mutate, setError } = useImportConnections({
    onSuccessConfirm: onClose,
  });

  const [actionIfExists, setActionIfExists] = useState<"fail" | "overwrite" | "skip">("fail");
  const [isParsing, setIsParsing] = useState(false);
  const [fileContent, setFileContent] = useState<ConnectionBody[] | undefined>(undefined);

  const onFileChange = (file: File) => {
    setIsParsing(true);
    setError(undefined);
    setFileContent(undefined);
    const reader = new FileReader();

    reader.addEventListener("load", (event) => {
      const text = event.target?.result as string;

      try {
        const parsed = parseConnectionsFile(text);
        const connections: ConnectionBody[] = [];

        for (const [connId, entry] of Object.entries(parsed)) {
          if (typeof entry === "object" && entry !== null && "conn_type" in entry) {
            connections.push(toConnectionBody(connId, entry as ConnectionExportEntry));
          }
        }

        setFileContent(connections);
      } catch {
        setError({
          body: {
            detail: translate("connections.import.errorParsingJsonFile"),
          },
        });
      } finally {
        setIsParsing(false);
      }
    });

    reader.readAsText(file);
  };

  const onSubmit = () => {
    setError(undefined);
    if (fileContent && fileContent.length > 0) {
      const formattedPayload: BulkBody_ConnectionBody_ = {
        actions: [
          {
            action: "create",
            action_on_existence: actionIfExists,
            entities: fileContent,
          },
        ],
      };

      mutate({ requestBody: formattedPayload });
    }
  };

  return (
    <>
      <FileUpload.Root
        accept={["application/json", "text/yaml", "text/x-yaml", ".json", ".yaml", ".yml"]}
        gap="1"
        maxFiles={1}
        mb={6}
        onFileChange={(files) => {
          if (files.acceptedFiles.length > 0 && files.acceptedFiles[0]) {
            onFileChange(files.acceptedFiles[0]);
          }
        }}
        required
      >
        <FileUpload.HiddenInput />
        <FileUpload.Label fontSize="md" mb={3}>
          {translate("connections.import.upload")}
        </FileUpload.Label>
        <FileUpload.Trigger asChild>
          <Button variant="outline">
            <LuFileUp /> {translate("connections.import.uploadPlaceholder")}
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
          {translate("connections.import.conflictResolution")}
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
        <Button
          colorPalette="brand"
          disabled={!Boolean(fileContent?.length) || isPending}
          onClick={onSubmit}
        >
          <FiUploadCloud /> {translate("connections.import.button")}
        </Button>
      </Box>
    </>
  );
};

export default ImportConnectionsForm;
