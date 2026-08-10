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
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { MdOutlineDifference, MdOutlineOpenInFull, MdWrapText } from "react-icons/md";
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
import Editor, { type EditorProps } from "src/components/MonacoEditor";
import Time from "src/components/Time";
import { Dialog, IconButton, ProgressBar } from "src/components/ui";
import { LazyClipboard } from "src/components/ui/LazyClipboard";
import { useMonacoTheme } from "src/context/colorMode";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useShortcut } from "src/hooks/useShortcut";
import { useConfig } from "src/queries/useConfig";
import { renderDuration } from "src/utils";

import { CodeDiffViewer } from "./CodeDiffViewer";
import { FileLocation } from "./FileLocation";
import { VersionCompareSelect } from "./VersionCompareSelect";

export const Code = () => {
  const { t: translate } = useTranslation(["dag", "common", "components"]);
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
  const [isFullscreen, setIsFullscreen] = useState(false);

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
  const toggleFullscreen = () => setIsFullscreen((prev) => !prev);
  const toggleCompareDropdown = () => setIsCompareDropdownOpen(!isCompareDropdownOpen);
  const exitDiffMode = () => {
    setCompareVersionNumber(undefined);
    setIsCompareDropdownOpen(false);
  };
  const handleVersionChange = (versionNumber: number) => {
    setCompareVersionNumber(versionNumber);
    setIsCompareDropdownOpen(false);
  };

  const { beforeMount, theme } = useMonacoTheme();

  useShortcut({
    ...SHORTCUTS.code.toggleFullscreen,
    callback: toggleFullscreen,
  });

  useShortcut({
    ...SHORTCUTS.code.toggleWrap,
    callback: toggleWrap,
  });

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

  const hasMultipleVersions = (dagVersions?.dag_versions.length ?? 0) >= 2;

  // Show an empty state on 404 instead of an error
  const displayedCode =
    codeError?.status === 404 && !Boolean(code?.content) ? translate("code.noCode") : (code?.content ?? "");
  const displayedCompareCode =
    compareCodeError?.status === 404 && !Boolean(compareCode?.content)
      ? translate("code.noCode")
      : (compareCode?.content ?? "");

  const codeStatus = (
    <>
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
    </>
  );

  const codeContent = isDiffMode ? (
    <Box dir="ltr" display="flex" flex={1} flexDirection="column" minH={0}>
      {dag?.fileloc !== undefined && (
        <FileLocation fileloc={dag.fileloc} relativeFileloc={dag.relative_fileloc} />
      )}
      <Box flex={1} minH={0}>
        <CodeDiffViewer modifiedCode={displayedCode} originalCode={displayedCompareCode} />
      </Box>
    </Box>
  ) : (
    <Box
      css={{
        "& *::selection": {
          bg: "gray.emphasized",
        },
      }}
      dir="ltr"
      display="flex"
      flex={1}
      flexDirection="column"
      fontSize="14px"
      minH={0}
    >
      {dag?.fileloc !== undefined && (
        <FileLocation fileloc={dag.fileloc} relativeFileloc={dag.relative_fileloc} />
      )}
      <Box flex={1} minH={0}>
        <Editor
          beforeMount={beforeMount}
          language="python"
          options={editorOptions}
          theme={theme}
          value={displayedCode}
        />
      </Box>
    </Box>
  );

  const actionButtons = (
    <HStack gap={1}>
      <IconButton
        aria-label={translate(`common:wrap.${wrap ? "un" : ""}wrap`)}
        label={translate("common:wrap.tooltip", { hotkey: "w" })}
        onClick={toggleWrap}
        variant={wrap ? "solid" : "ghost"}
      >
        <MdWrapText />
      </IconButton>
      {hasMultipleVersions ? (
        <IconButton
          aria-label={translate("common:diff")}
          label={translate("common:diff")}
          onClick={toggleCompareDropdown}
          variant={isCompareDropdownOpen || isDiffMode ? "solid" : "ghost"}
        >
          <MdOutlineDifference />
        </IconButton>
      ) : undefined}
      {isFullscreen ? undefined : (
        <IconButton
          label={translate("common:fullscreen.tooltip", { hotkey: "f" })}
          onClick={toggleFullscreen}
        >
          <MdOutlineOpenInFull />
        </IconButton>
      )}
      <LazyClipboard
        getValue={() => code?.content ?? ""}
        label={translate("components:clipboard.copy")}
        size="md"
        variant="ghost"
      />
    </HStack>
  );

  const codeHeader = (
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
          {actionButtons}
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
              placeholder={translate("common:diffSelectVersionToCompare")}
              selectedVersionNumber={compareVersionNumber}
            />
          </Box>
        ) : undefined}
      </VStack>
    </HStack>
  );

  return (
    <Box display="flex" flexDirection="column" h="100%" overflow="hidden">
      {codeHeader}
      {codeStatus}
      {codeContent}
      <Dialog.Root
        onOpenChange={() => setIsFullscreen(false)}
        open={isFullscreen}
        scrollBehavior="inside"
        size="full"
      >
        {isFullscreen ? (
          // size="full" gives the content only a min-height, which is not a definite
          // height for flex children — without an explicit height the editor collapses.
          <Dialog.Content backdrop h="100dvh">
            <Dialog.Header width="100%">
              <Box display="flex" flexDirection="column" width="100%">
                <Heading size="xl">{dagId}</Heading>
                {codeHeader}
              </Box>
            </Dialog.Header>

            <Dialog.CloseTrigger />

            <Dialog.Body display="flex" flexDirection="column" minH={0}>
              {codeStatus}
              {codeContent}
            </Dialog.Body>
          </Dialog.Content>
        ) : undefined}
      </Dialog.Root>
    </Box>
  );
};
