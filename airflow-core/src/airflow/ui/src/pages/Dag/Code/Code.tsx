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
import { Box, Button, Heading, HStack, Link, VStack } from "@chakra-ui/react";
import Editor, { type EditorProps } from "@monaco-editor/react";
import { useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagSourceServiceGetDagSource,
  useDagVersionServiceGetDagVersion,
  useDagVersionServiceGetDagVersions,
} from "openapi/queries";
import type { ApiError } from "openapi/requests/core/ApiError";
import type { DAGSourceResponse } from "openapi/requests/types.gen";
import { DagVersionSelect } from "src/components/DagVersionSelect";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardButton, Tooltip } from "src/components/ui";
import { ProgressBar } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useConfig } from "src/queries/useConfig";
import { renderDuration } from "src/utils";

import { CodeDiffViewer } from "./CodeDiffViewer";
import { VersionCompareSelect } from "./VersionCompareSelect";

export const Code = () => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const { dagId } = useParams();

  const selectedVersion = useSelectedVersion();

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId: dagId ?? "",
  });

  const { data: dagVersion } = useDagVersionServiceGetDagVersion(
    {
      dagId: dagId ?? "",
      versionNumber: selectedVersion ?? 1,
    },
    undefined,
    { enabled: dag !== undefined && selectedVersion !== undefined },
  );

  const { data: dagVersions } = useDagVersionServiceGetDagVersions({
    dagId: dagId ?? "",
  });

  const defaultWrap = Boolean(useConfig("default_wrap"));

  const [wrap, setWrap] = useState(defaultWrap);
  const [compareVersionNumber, setCompareVersionNumber] = useState<number | undefined>(undefined);
  const [isCompareDropdownOpen, setIsCompareDropdownOpen] = useState(false);

  const isDiffMode = compareVersionNumber !== undefined;

  const {
    data: code,
    error: codeError,
    isLoading: isCodeLoading,
  } = useDagSourceServiceGetDagSource<DAGSourceResponse, ApiError | null>({
    dagId: dagId ?? "",
    versionNumber: selectedVersion,
  });

  const {
    data: compareCode,
    error: compareCodeError,
    isLoading: isCompareCodeLoading,
  } = useDagSourceServiceGetDagSource<DAGSourceResponse, ApiError | null>(
    {
      dagId: dagId ?? "",
      versionNumber: compareVersionNumber,
    },
    undefined,
    { enabled: isDiffMode },
  );

  const toggleWrap = () => setWrap(!wrap);
  const toggleCompareDropdown = () => setIsCompareDropdownOpen(!isCompareDropdownOpen);
  const exitDiffMode = () => {
    setCompareVersionNumber(undefined);
    setIsCompareDropdownOpen(false);
  };
  const handleVersionChange = (versionNumber: number) => {
    setCompareVersionNumber(versionNumber);
    setIsCompareDropdownOpen(false);
  };

  const { colorMode } = useColorMode();

  useHotkeys("w", toggleWrap);

  const editorOptions: EditorProps["options"] = {
    automaticLayout: true,
    contextmenu: false,
    find: {
      addExtraSpaceOnTop: false,
      autoFindInSelection: "never" as const,
      seedSearchStringFromSelection: "always" as const,
    },
    fontSize: 14,
    glyphMargin: false,
    lineDecorationsWidth: 20,
    lineNumbers: "on",
    minimap: { enabled: false },
    readOnly: true,
    renderLineHighlight: "none",
    wordWrap: wrap ? "on" : "off",
  };

  const theme = colorMode === "dark" ? "vs-dark" : "vs-light";

  const hasMultipleVersions = (dagVersions?.dag_versions.length ?? 0) >= 2;

  return (
    <Box h="100%" overflow="hidden">
      <HStack justifyContent="space-between" mt={2}>
        <HStack gap={5}>
          {dag?.last_parsed_time !== undefined && (
            <Heading as="h4" fontSize="14px" size="md">
              {translate("code.parsedAt")} <Time datetime={dag.last_parsed_time} />
            </Heading>
          )}
          {dag?.last_parse_duration !== undefined && (
            <Heading as="h4" fontSize="14px" size="md">
              {translate("code.parseDuration")} {renderDuration(dag.last_parse_duration)}
            </Heading>
          )}

          {dagVersion !== undefined && dagVersion.bundle_version !== null ? (
            <Heading as="h4" fontSize="14px" size="md" wordBreak="break-word">
              {translate("dagDetails.bundleVersion")}
              {": "}
              {dagVersion.bundle_url === null ? (
                dagVersion.bundle_version
              ) : (
                <Link
                  aria-label={translate("code.bundleUrl")}
                  color="fg.info"
                  href={dagVersion.bundle_url}
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  {dagVersion.bundle_version}
                </Link>
              )}
            </Heading>
          ) : undefined}
        </HStack>
        <VStack gap={2} position="relative">
          <HStack flexWrap="wrap" gap={2}>
            <DagVersionSelect showLabel={false} />
            <ClipboardRoot value={code?.content ?? ""}>
              <ClipboardButton />
            </ClipboardRoot>
            <Tooltip
              closeDelay={100}
              content={translate("common:wrap.tooltip", { hotkey: "w" })}
              openDelay={100}
            >
              <Button
                aria-label={translate(`common:wrap.${wrap ? "un" : ""}wrap`)}
                onClick={toggleWrap}
                variant="outline"
              >
                {translate(`common:wrap.${wrap ? "un" : ""}wrap`)}
              </Button>
            </Tooltip>
            {hasMultipleVersions ? (
              <Button
                aria-label={translate("common:diff")}
                onClick={toggleCompareDropdown}
                variant={isCompareDropdownOpen ? "solid" : "outline"}
              >
                {translate("common:diff")}
              </Button>
            ) : undefined}
            {isDiffMode ? (
              <Button aria-label={translate("common:diffExit")} onClick={exitDiffMode} variant="solid">
                {translate("common:diffExit")}
              </Button>
            ) : undefined}
          </HStack>
          {isCompareDropdownOpen ? (
            <Box
              bg="bg.panel"
              borderRadius="md"
              insetInlineEnd={0}
              mt={4}
              p={2}
              position="absolute"
              shadow="sm"
              top="100%"
              zIndex={10}
            >
              <VersionCompareSelect
                label={translate("common:diffCompareWith")}
                onVersionChange={handleVersionChange}
                placeholder="Select version to compare"
                selectedVersionNumber={compareVersionNumber}
              />
            </Box>
          ) : undefined}
        </VStack>
      </HStack>
      {/* We want to show an empty state on 404 instead of an error */}
      <ErrorAlert
        error={
          error ??
          (codeError?.status === 404 ? undefined : codeError) ??
          (compareCodeError?.status === 404 ? undefined : compareCodeError)
        }
      />
      <ProgressBar
        size="xs"
        visibility={isLoading || isCodeLoading || isCompareCodeLoading ? "visible" : "hidden"}
      />

      {isDiffMode ? (
        <Box dir="ltr" height="full">
          <CodeDiffViewer
            modifiedCode={
              codeError?.status === 404 && !Boolean(code?.content)
                ? translate("code.noCode")
                : (code?.content ?? "")
            }
            originalCode={
              compareCodeError?.status === 404 && !Boolean(compareCode?.content)
                ? translate("code.noCode")
                : (compareCode?.content ?? "")
            }
          />
        </Box>
      ) : (
        <Box
          css={{
            "& *::selection": {
              bg: "gray.emphasized",
            },
          }}
          dir="ltr"
          fontSize="14px"
          height="full"
        >
          <Editor
            language="python"
            options={editorOptions}
            theme={theme}
            value={
              codeError?.status === 404 && !Boolean(code?.content)
                ? translate("code.noCode")
                : (code?.content ?? "")
            }
          />
        </Box>
      )}
    </Box>
  );
};
