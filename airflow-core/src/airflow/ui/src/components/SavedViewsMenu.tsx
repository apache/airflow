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
import { Box, Button, HStack, Input, Text, VStack } from "@chakra-ui/react";
import type { SortingState } from "@tanstack/react-table";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";
import { LuBookmark, LuInfo } from "react-icons/lu";
import { MdOutlinePushPin, MdPushPin } from "react-icons/md";
import { useLocation, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import DeleteDialog from "src/components/DeleteDialog";
import { IconButton, Popover, Tooltip } from "src/components/ui";
import { savedViewsDefaultKey, savedViewsKey, tableSortKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

type SavedView = {
  readonly name: string;
  readonly search: string;
};

const normalizeSearch = (value: string) => {
  const params = new URLSearchParams(value);

  params.sort();

  return params.toString();
};

export const SavedViewsMenu = () => {
  const { t: translate } = useTranslation("common");
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [sorting, setSorting] = useLocalStorage<SortingState>(tableSortKey(pathname), []);
  const [savedViews, setSavedViews] = useLocalStorage<Array<SavedView>>(savedViewsKey(pathname), []);
  const [defaultViewName, setDefaultViewName] = useLocalStorage<string | null>(
    savedViewsDefaultKey(pathname),
    null,
  );
  const [name, setName] = useState("");
  const [open, setOpen] = useState(false);
  const [viewToDelete, setViewToDelete] = useState<string | undefined>(undefined);

  const viewParams = new URLSearchParams(searchParams);

  viewParams.delete(SearchParamsKeys.OFFSET);
  viewParams.delete(SearchParamsKeys.CURSOR);
  // Only URL filters count, so the bare page blocks save even when a sort sits in localStorage.
  const hasViewToSave = [...viewParams].length > 0;

  // The sort is often only in localStorage, not the URL — bake it into the snapshot so it is restored.
  if (viewParams.getAll(SearchParamsKeys.SORT).length === 0) {
    sorting.forEach(({ desc, id }) => viewParams.append(SearchParamsKeys.SORT, `${desc ? "-" : ""}${id}`));
  }
  const search = viewParams.toString();

  const duplicateView = savedViews.find((view) => normalizeSearch(view.search) === normalizeSearch(search));

  const handleSave = () => {
    const trimmedName = name.trim();

    if (trimmedName === "" || !hasViewToSave || duplicateView !== undefined) {
      return;
    }

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
    // Keep the sort out of the URL (it lives in localStorage), or it lingers as a `sort` param after reset.
    params.delete(SearchParamsKeys.SORT);
    setSearchParams(params);
    setOpen(false);
  };

  const deleteView = (viewName: string) => {
    setSavedViews((prev) => prev.filter((view) => view.name !== viewName));
    setDefaultViewName((previous) => (previous === viewName ? null : previous));
  };

  const toggleDefault = (viewName: string) => {
    setDefaultViewName((previous) => (previous === viewName ? null : viewName));
  };

  // Restore the default view on a filterless landing. Keyed on pathname so the user's later edits/clears
  // don't re-trigger it and a deep link's filters win; sort and pagination don't count toward "filterless".
  useEffect(() => {
    const target =
      defaultViewName === null ? undefined : savedViews.find((view) => view.name === defaultViewName);
    const filters = new URLSearchParams(searchParams);

    filters.delete(SearchParamsKeys.OFFSET);
    filters.delete(SearchParamsKeys.CURSOR);
    filters.delete(SearchParamsKeys.SORT);

    if (target !== undefined && [...filters].length === 0) {
      applyView(target);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname]);

  let saveHint: string | undefined;

  if (!hasViewToSave) {
    saveHint = translate("savedViews.nothingToSave");
  } else if (duplicateView !== undefined) {
    saveHint = translate("savedViews.duplicate", { name: duplicateView.name });
  }

  const infoContent = (
    <VStack alignItems="start" gap={2} maxW="260px">
      <Text fontSize="xs">{translate("savedViews.info.save")}</Text>
      <Text fontSize="xs">{translate("savedViews.info.default")}</Text>
      <Text fontSize="xs">{translate("savedViews.info.storage")}</Text>
    </VStack>
  );

  return (
    <>
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
          <VStack align="stretch" gap={2} p={4}>
            <HStack gap={1}>
              <Text fontSize="sm" fontWeight="medium">
                {translate("savedViews.title")}
              </Text>
              <Tooltip content={infoContent}>
                <Box as="span" color="fg.muted" cursor="pointer">
                  <LuInfo />
                </Box>
              </Tooltip>
            </HStack>
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
                value={name}
              />
              <Tooltip content={saveHint ?? ""} disabled={saveHint === undefined}>
                <Box>
                  <Button
                    data-testid="saved-view-save"
                    disabled={!hasViewToSave || name.trim() === "" || duplicateView !== undefined}
                    onClick={handleSave}
                    size="sm"
                  >
                    {translate("savedViews.save")}
                  </Button>
                </Box>
              </Tooltip>
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
                    label={
                      defaultViewName === view.name
                        ? translate("savedViews.unsetDefault")
                        : translate("savedViews.setDefault")
                    }
                    onClick={() => toggleDefault(view.name)}
                    size="sm"
                    variant="ghost"
                  >
                    {defaultViewName === view.name ? <MdPushPin /> : <MdOutlinePushPin />}
                  </IconButton>
                  <IconButton
                    aria-label="Delete view"
                    onClick={() => setViewToDelete(view.name)}
                    size="sm"
                    variant="ghost"
                  >
                    <FiTrash2 />
                  </IconButton>
                </HStack>
              ))
            )}
          </VStack>
        </Popover.Content>
      </Popover.Root>
      <DeleteDialog
        isDeleting={false}
        onClose={() => setViewToDelete(undefined)}
        onDelete={() => {
          if (viewToDelete !== undefined) {
            deleteView(viewToDelete);
          }
          setViewToDelete(undefined);
        }}
        open={viewToDelete !== undefined}
        resourceName={viewToDelete ?? ""}
        title={translate("savedViews.deleteTitle")}
        warningText={translate("savedViews.deleteWarning")}
      />
    </>
  );
};
