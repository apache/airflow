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
import { Box, Flex, Heading, Skeleton, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiBox, FiChevronDown, FiChevronRight, FiFolder } from "react-icons/fi";

import type { DagFolderResponse } from "openapi/requests/types.gen";

import { groupFoldersByBundle, type BundleFolders, type FolderNode } from "./buildFolderTree";

export type FolderSelection = {
  readonly bundleName: string | undefined;
  readonly folder: string | undefined;
};

type Props = {
  readonly folders: ReadonlyArray<DagFolderResponse>;
  readonly isLoading?: boolean;
  readonly onSelectFolder: (selection: FolderSelection) => void;
  readonly selectedBundle: string | undefined;
  readonly selectedFolder: string | undefined;
};

const folderKey = (bundleName: string | undefined, path: string) => `folder:${bundleName ?? ""}:${path}`;
const bundleKey = (bundleName: string) => `bundle:${bundleName}`;

// Ancestor folder paths of the selected folder, so the tree opens to reveal the selection.
const ancestorPaths = (folder: string | undefined): Array<string> => {
  if (folder === undefined || folder === "") {
    return [];
  }

  const segments = folder.split("/");

  return segments.map((_, index) => segments.slice(0, index + 1).join("/"));
};

const initialExpanded = (
  bundleName: string | undefined,
  selectedFolder: string | undefined,
  isMultiBundle: boolean,
): Array<string> => {
  const keys = ancestorPaths(selectedFolder).map((path) => folderKey(bundleName, path));

  if (isMultiBundle && bundleName !== undefined) {
    keys.push(bundleKey(bundleName));
  }

  return keys;
};

type FolderRowProps = {
  readonly bundleName: string | undefined;
  readonly depthOffset: number;
  readonly expanded: Set<string>;
  readonly node: FolderNode;
  readonly onSelectFolder: (selection: FolderSelection) => void;
  readonly onToggle: (key: string) => void;
  readonly selectedBundle: string | undefined;
  readonly selectedFolder: string | undefined;
};

const FolderRow = ({
  bundleName,
  depthOffset,
  expanded,
  node,
  onSelectFolder,
  onToggle,
  selectedBundle,
  selectedFolder,
}: FolderRowProps) => {
  const hasChildren = node.children.length > 0;
  const key = folderKey(bundleName, node.path);
  const isExpanded = expanded.has(key);
  const isSelected = selectedBundle === bundleName && selectedFolder === node.path;
  const depth = node.path.split("/").length - 1 + depthOffset;

  return (
    <Box>
      <Flex
        _hover={{ bg: isSelected ? "blue.subtle" : "bg.muted" }}
        alignItems="center"
        bg={isSelected ? "blue.subtle" : undefined}
        borderRadius="sm"
        cursor="pointer"
        gap={1}
        onClick={() => onSelectFolder({ bundleName, folder: node.path })}
        pl={`${depth * 16 + 4}px`}
        py={1}
      >
        <Box
          aria-label={isExpanded ? "Collapse" : "Expand"}
          as="span"
          color="fg.muted"
          onClick={(event) => {
            event.stopPropagation();
            if (hasChildren) {
              onToggle(key);
            }
          }}
          visibility={hasChildren ? "visible" : "hidden"}
        >
          {isExpanded ? <FiChevronDown /> : <FiChevronRight />}
        </Box>
        <Box as="span" color="fg.muted">
          <FiFolder />
        </Box>
        <Text fontWeight={isSelected ? "bold" : "normal"} truncate>
          {node.name}
        </Text>
      </Flex>
      {hasChildren && isExpanded ? (
        <Box>
          {node.children.map((child) => (
            <FolderRow
              bundleName={bundleName}
              depthOffset={depthOffset}
              expanded={expanded}
              key={child.path}
              node={child}
              onSelectFolder={onSelectFolder}
              onToggle={onToggle}
              selectedBundle={selectedBundle}
              selectedFolder={selectedFolder}
            />
          ))}
        </Box>
      ) : undefined}
    </Box>
  );
};

type BundleRowProps = {
  readonly bundle: BundleFolders;
  readonly expanded: Set<string>;
  readonly onSelectFolder: (selection: FolderSelection) => void;
  readonly onToggle: (key: string) => void;
  readonly selectedBundle: string | undefined;
  readonly selectedFolder: string | undefined;
};

const BundleRow = ({
  bundle,
  expanded,
  onSelectFolder,
  onToggle,
  selectedBundle,
  selectedFolder,
}: BundleRowProps) => {
  const key = bundleKey(bundle.bundleName);
  const isExpanded = expanded.has(key);
  const isSelected = selectedBundle === bundle.bundleName && selectedFolder === undefined;
  const hasChildren = bundle.tree.length > 0;

  return (
    <Box>
      <Flex
        _hover={{ bg: isSelected ? "blue.subtle" : "bg.muted" }}
        alignItems="center"
        bg={isSelected ? "blue.subtle" : undefined}
        borderRadius="sm"
        cursor="pointer"
        gap={1}
        onClick={() => onSelectFolder({ bundleName: bundle.bundleName, folder: undefined })}
        pl="4px"
        py={1}
      >
        <Box
          aria-label={isExpanded ? "Collapse" : "Expand"}
          as="span"
          color="fg.muted"
          onClick={(event) => {
            event.stopPropagation();
            if (hasChildren) {
              onToggle(key);
            }
          }}
          visibility={hasChildren ? "visible" : "hidden"}
        >
          {isExpanded ? <FiChevronDown /> : <FiChevronRight />}
        </Box>
        <Box as="span" color="fg.muted">
          <FiBox />
        </Box>
        <Text fontWeight={isSelected ? "bold" : "normal"} truncate>
          {bundle.bundleName}
        </Text>
      </Flex>
      {hasChildren && isExpanded ? (
        <Box>
          {bundle.tree.map((node) => (
            <FolderRow
              bundleName={bundle.bundleName}
              depthOffset={1}
              expanded={expanded}
              key={node.path}
              node={node}
              onSelectFolder={onSelectFolder}
              onToggle={onToggle}
              selectedBundle={selectedBundle}
              selectedFolder={selectedFolder}
            />
          ))}
        </Box>
      ) : undefined}
    </Box>
  );
};

export const DagFolderTree = ({
  folders,
  isLoading = false,
  onSelectFolder,
  selectedBundle,
  selectedFolder,
}: Props) => {
  const { t: translate } = useTranslation("dags");
  const bundles = groupFoldersByBundle(folders);
  const isMultiBundle = bundles.length > 1;

  const [expanded, setExpanded] = useState<Set<string>>(
    () => new Set(initialExpanded(isMultiBundle ? selectedBundle : undefined, selectedFolder, isMultiBundle)),
  );

  const handleToggle = (key: string) => {
    setExpanded((previous) => {
      const next = new Set(previous);

      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }

      return next;
    });
  };

  const isAllSelected = selectedBundle === undefined && selectedFolder === undefined;

  return (
    <Box minWidth="220px">
      <Heading mb={2} size="sm">
        {translate("folders.title")}
      </Heading>
      {isLoading ? (
        <VStack align="stretch" gap={2}>
          <Skeleton height="24px" />
          <Skeleton height="24px" />
          <Skeleton height="24px" />
        </VStack>
      ) : (
        <Box>
          <Flex
            _hover={{ bg: isAllSelected ? "blue.subtle" : "bg.muted" }}
            alignItems="center"
            bg={isAllSelected ? "blue.subtle" : undefined}
            borderRadius="sm"
            cursor="pointer"
            gap={1}
            onClick={() => onSelectFolder({ bundleName: undefined, folder: undefined })}
            pl="4px"
            py={1}
          >
            <Text fontWeight={isAllSelected ? "bold" : "normal"}>{translate("folders.all")}</Text>
          </Flex>
          {bundles.length === 0 ? (
            <Text color="fg.muted" fontSize="sm" pl="4px" py={1}>
              {translate("folders.empty")}
            </Text>
          ) : isMultiBundle ? (
            bundles.map((bundle) => (
              <BundleRow
                bundle={bundle}
                expanded={expanded}
                key={bundle.bundleName}
                onSelectFolder={onSelectFolder}
                onToggle={handleToggle}
                selectedBundle={selectedBundle}
                selectedFolder={selectedFolder}
              />
            ))
          ) : (
            bundles[0]?.tree.map((node) => (
              <FolderRow
                bundleName={undefined}
                depthOffset={0}
                expanded={expanded}
                key={node.path}
                node={node}
                onSelectFolder={onSelectFolder}
                onToggle={handleToggle}
                selectedBundle={selectedBundle}
                selectedFolder={selectedFolder}
              />
            ))
          )}
        </Box>
      )}
    </Box>
  );
};
