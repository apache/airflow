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
import { FiChevronDown, FiChevronRight, FiFolder } from "react-icons/fi";

import { buildFolderTree, type FolderNode } from "./buildFolderTree";

type Props = {
  readonly folders: ReadonlyArray<string>;
  readonly isLoading?: boolean;
  readonly onSelectFolder: (path: string | undefined) => void;
  readonly selectedFolder: string | undefined;
};

// Every ancestor folder of the selected path, so the tree opens to reveal the selection.
const ancestorPaths = (folder: string | undefined): Array<string> => {
  if (folder === undefined || folder === "") {
    return [];
  }

  const segments = folder.split("/");

  return segments.map((_, index) => segments.slice(0, index + 1).join("/"));
};

type RowProps = {
  readonly expanded: Set<string>;
  readonly node: FolderNode;
  readonly onSelectFolder: (path: string | undefined) => void;
  readonly onToggle: (path: string) => void;
  readonly selectedFolder: string | undefined;
};

const FolderRow = ({ expanded, node, onSelectFolder, onToggle, selectedFolder }: RowProps) => {
  const hasChildren = node.children.length > 0;
  const isExpanded = expanded.has(node.path);
  const isSelected = selectedFolder === node.path;
  const depth = node.path.split("/").length - 1;

  return (
    <Box>
      <Flex
        _hover={{ bg: isSelected ? "blue.subtle" : "bg.muted" }}
        alignItems="center"
        bg={isSelected ? "blue.subtle" : undefined}
        borderRadius="sm"
        cursor="pointer"
        gap={1}
        onClick={() => onSelectFolder(node.path)}
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
              onToggle(node.path);
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
              expanded={expanded}
              key={child.path}
              node={child}
              onSelectFolder={onSelectFolder}
              onToggle={onToggle}
              selectedFolder={selectedFolder}
            />
          ))}
        </Box>
      ) : undefined}
    </Box>
  );
};

export const DagFolderTree = ({ folders, isLoading = false, onSelectFolder, selectedFolder }: Props) => {
  const { t: translate } = useTranslation("dags");
  const tree = buildFolderTree(folders);

  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(ancestorPaths(selectedFolder)));

  const handleToggle = (path: string) => {
    setExpanded((previous) => {
      const next = new Set(previous);

      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }

      return next;
    });
  };

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
            _hover={{ bg: selectedFolder === undefined ? "blue.subtle" : "bg.muted" }}
            alignItems="center"
            bg={selectedFolder === undefined ? "blue.subtle" : undefined}
            borderRadius="sm"
            cursor="pointer"
            gap={1}
            onClick={() => onSelectFolder(undefined)}
            pl="4px"
            py={1}
          >
            <Text fontWeight={selectedFolder === undefined ? "bold" : "normal"}>
              {translate("folders.all")}
            </Text>
          </Flex>
          {tree.length === 0 ? (
            <Text color="fg.muted" fontSize="sm" pl="4px" py={1}>
              {translate("folders.empty")}
            </Text>
          ) : (
            tree.map((node) => (
              <FolderRow
                expanded={expanded}
                key={node.path}
                node={node}
                onSelectFolder={onSelectFolder}
                onToggle={handleToggle}
                selectedFolder={selectedFolder}
              />
            ))
          )}
        </Box>
      )}
    </Box>
  );
};
