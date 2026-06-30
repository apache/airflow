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
  Box,
  Button,
  createTreeCollection,
  Heading,
  Skeleton,
  TreeView,
  type TreeViewNodeRenderProps,
  VStack,
} from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiBox, FiChevronRight, FiFolder } from "react-icons/fi";

import type { DagFolderResponse } from "openapi/requests/types.gen";

import {
  bundleNodeValue,
  buildFolderTreeNodes,
  folderNodeValue,
  type FolderTreeNode,
} from "./buildFolderTree";

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

const NodeIcon = ({ node }: { readonly node: FolderTreeNode }) => (
  <Box as="span" color="fg.muted" flexShrink={0}>
    {node.folder === undefined ? <FiBox /> : <FiFolder />}
  </Box>
);

// Rows span the full panel width and long names are truncated, so the selected background always
// covers the whole row instead of stopping where the text overflows. The full name stays available
// as a native tooltip.
const renderNode = (node: FolderTreeNode, isBranch: boolean) =>
  isBranch ? (
    <TreeView.BranchControl width="100%">
      <TreeView.BranchIndicator flexShrink={0}>
        <FiChevronRight />
      </TreeView.BranchIndicator>
      <NodeIcon node={node} />
      <TreeView.BranchText minWidth={0} title={node.label} truncate>
        {node.label}
      </TreeView.BranchText>
    </TreeView.BranchControl>
  ) : (
    <TreeView.Item width="100%">
      {/* Reserve the width of the expand indicator so leaves line up with their sibling folders. */}
      <Box aria-hidden as="span" flexShrink={0} visibility="hidden">
        <FiChevronRight />
      </Box>
      <NodeIcon node={node} />
      <TreeView.ItemText minWidth={0} title={node.label} truncate>
        {node.label}
      </TreeView.ItemText>
    </TreeView.Item>
  );

// Ancestor folders of the selection, so the tree opens far enough to reveal it.
const expandedForSelection = (
  bundleName: string | undefined,
  folder: string | undefined,
  isMultiBundle: boolean,
): Array<string> => {
  const values: Array<string> = [];

  if (isMultiBundle && bundleName !== undefined) {
    values.push(bundleNodeValue(bundleName));
  }

  if (folder !== undefined && folder !== "" && bundleName !== undefined) {
    const segments = folder.split("/");

    values.push(
      ...segments.map((_, index) => folderNodeValue(bundleName, segments.slice(0, index + 1).join("/"))),
    );
  }

  return values;
};

export const DagFolderTree = ({
  folders,
  isLoading = false,
  onSelectFolder,
  selectedBundle,
  selectedFolder,
}: Props) => {
  const { t: translate } = useTranslation("dags");
  const nodes = buildFolderTreeNodes(folders);
  // Bundle nodes are only emitted when there is more than one bundle.
  const isMultiBundle = nodes.some((node) => node.folder === undefined);
  // Node values always carry a bundle; with a single bundle the URL omits it, so fall back to
  // the only bundle present to keep selection and expansion pointing at the right nodes.
  const nodeBundle = isMultiBundle ? selectedBundle : nodes[0]?.bundleName;

  const [expandedValue, setExpandedValue] = useState<Array<string>>(() =>
    expandedForSelection(nodeBundle, selectedFolder, isMultiBundle),
  );

  // Folders arrive asynchronously, so the first render has no nodes to expand yet: re-apply the
  // auto-expansion whenever the selection resolves to different nodes (on load, or when the user
  // lands on a URL that already points at a nested folder). Manual expansions are kept.
  const selectionKey = `${nodeBundle ?? ""}|${selectedFolder ?? ""}|${isMultiBundle}`;
  const [appliedSelectionKey, setAppliedSelectionKey] = useState(selectionKey);

  if (appliedSelectionKey !== selectionKey) {
    setAppliedSelectionKey(selectionKey);
    setExpandedValue((previous) => [
      ...new Set([...previous, ...expandedForSelection(nodeBundle, selectedFolder, isMultiBundle)]),
    ]);
  }

  const collection = createTreeCollection<FolderTreeNode>({
    rootNode: { bundleName: "", children: nodes, label: "", value: "__root__" },
  });

  const selectedValue =
    selectedFolder !== undefined && nodeBundle !== undefined
      ? [folderNodeValue(nodeBundle, selectedFolder)]
      : selectedBundle === undefined
        ? []
        : [bundleNodeValue(selectedBundle)];

  const isAllSelected = selectedBundle === undefined && selectedFolder === undefined;

  return (
    <Box minWidth={0} width="100%">
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
          <Button
            bg={isAllSelected ? "blue.subtle" : undefined}
            fontWeight={isAllSelected ? "bold" : "normal"}
            justifyContent="flex-start"
            onClick={() => onSelectFolder({ bundleName: undefined, folder: undefined })}
            size="sm"
            variant="ghost"
            width="100%"
          >
            {translate("folders.all")}
          </Button>
          {nodes.length === 0 ? (
            <Box color="fg.muted" fontSize="sm" pl="4px" py={1}>
              {translate("folders.empty")}
            </Box>
          ) : (
            <TreeView.Root
              collection={collection}
              expandedValue={expandedValue}
              onExpandedChange={(details) => setExpandedValue(details.expandedValue)}
              onSelectionChange={(details) => {
                const [value] = details.selectedValue;
                const selectedNode = value === undefined ? undefined : collection.findNode(value);

                if (selectedNode !== undefined) {
                  onSelectFolder({ bundleName: selectedNode.bundleName, folder: selectedNode.folder });
                }
              }}
              selectedValue={selectedValue}
              selectionMode="single"
              size="sm"
            >
              <TreeView.Tree>
                <TreeView.Node
                  indentGuide={<TreeView.BranchIndentGuide />}
                  render={({ node, nodeState }: TreeViewNodeRenderProps<FolderTreeNode>) =>
                    renderNode(node, nodeState.isBranch)
                  }
                />
              </TreeView.Tree>
            </TreeView.Root>
          )}
        </Box>
      )}
    </Box>
  );
};
