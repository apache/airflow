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
import { Button, HStack, Input, Text, VStack } from "@chakra-ui/react";
import type { SortingState } from "@tanstack/react-table";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";
import { LuBookmark } from "react-icons/lu";
import { useLocation, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { IconButton, Popover } from "src/components/ui";
import { savedViewsKey, tableSortKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

type SavedView = {
  readonly name: string;
  readonly search: string;
};

// A "view" is the table's current URL query string — filters, search and page size all live there.
// Sorting is mirrored to localStorage and is frequently absent from the URL, so it is baked into the
// snapshot explicitly. Pagination position (offset/cursor) is dropped so a restored view starts on page one.
export const SavedViewsMenu = () => {
  const { t: translate } = useTranslation("common");
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [sorting, setSorting] = useLocalStorage<SortingState>(tableSortKey(pathname), []);
  const [savedViews, setSavedViews] = useLocalStorage<Array<SavedView>>(savedViewsKey(pathname), []);
  const [name, setName] = useState("");
  const [open, setOpen] = useState(false);

  const handleSave = () => {
    const trimmedName = name.trim();

    if (trimmedName === "") {
      return;
    }

    const params = new URLSearchParams(searchParams);

    params.delete(SearchParamsKeys.OFFSET);
    params.delete(SearchParamsKeys.CURSOR);
    // The active sort lives in the URL when set there, otherwise only in localStorage — bake it in
    // either way so a restored view orders the table the same as when it was saved.
    if (params.getAll(SearchParamsKeys.SORT).length === 0) {
      sorting.forEach(({ desc, id }) => params.append(SearchParamsKeys.SORT, `${desc ? "-" : ""}${id}`));
    }
    const search = params.toString();

    setSavedViews((prev) =>
      prev.some((view) => view.name === trimmedName)
        ? prev.map((view) => (view.name === trimmedName ? { name: trimmedName, search } : view))
        : [...prev, { name: trimmedName, search }],
    );
    setName("");
  };

  const applyView = (view: SavedView) => {
    const params = new URLSearchParams(view.search);

    setSorting(
      params
        .getAll(SearchParamsKeys.SORT)
        .map((sort) => ({ desc: sort.startsWith("-"), id: sort.replace("-", "") })),
    );
    setSearchParams(params);
    setOpen(false);
  };

  const deleteView = (viewName: string) => {
    setSavedViews((prev) => prev.filter((view) => view.name !== viewName));
  };

  return (
    <Popover.Root
      lazyMount
      onOpenChange={(event) => setOpen(event.open)}
      open={open}
      positioning={{ placement: "bottom-start" }}
      unmountOnExit
    >
      <Popover.Trigger asChild>
        <Button borderRadius="full" colorPalette="gray" data-testid="saved-views-button" variant="outline">
          <LuBookmark />
          {translate("savedViews.title")}
        </Button>
      </Popover.Trigger>
      <Popover.Content width="xs">
        <Popover.Arrow />
        <Popover.Body>
          <VStack align="stretch" gap={2}>
            <HStack>
              <Input
                data-testid="saved-view-name"
                onChange={(event) => setName(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handleSave();
                  }
                }}
                placeholder={translate("savedViews.namePlaceholder")}
                size="sm"
              />
              <Button
                data-testid="saved-view-save"
                disabled={name.trim() === ""}
                onClick={handleSave}
                size="sm"
              >
                {translate("savedViews.save")}
              </Button>
            </HStack>
            {savedViews.length === 0 ? (
              <Text color="fg.muted" fontSize="sm" py={1}>
                {translate("savedViews.empty")}
              </Text>
            ) : (
              savedViews.map((view) => (
                <HStack gap={1} key={view.name}>
                  <Button
                    flex="1"
                    justifyContent="flex-start"
                    minW={0}
                    onClick={() => applyView(view)}
                    size="sm"
                    variant="ghost"
                  >
                    <Text truncate>{view.name}</Text>
                  </Button>
                  <IconButton
                    aria-label="Delete view"
                    onClick={() => deleteView(view.name)}
                    size="sm"
                    variant="ghost"
                  >
                    <FiTrash2 />
                  </IconButton>
                </HStack>
              ))
            )}
          </VStack>
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
