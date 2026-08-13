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
  <Box as="span" color="fg.muted">
    {node.folder === undefined ? <FiBox /> : <FiFolder />}
  </Box>
);

const renderNode = (node: FolderTreeNode, isBranch: boolean) =>
  isBranch ? (
    <TreeView.BranchControl>
      <TreeView.BranchIndicator>
        <FiChevronRight />
      </TreeView.BranchIndicator>
      <NodeIcon node={node} />
      <TreeView.BranchText>{node.label}</TreeView.BranchText>
    </TreeView.BranchControl>
  ) : (
    <TreeView.Item>
      {/* Reserve the width of the expand indicator so leaves line up with their sibling folders. */}
      <Box aria-hidden as="span" visibility="hidden">
        <FiChevronRight />
      </Box>
      <NodeIcon node={node} />
      <TreeView.ItemText>{node.label}</TreeView.ItemText>
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
