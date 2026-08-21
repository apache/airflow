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
import { presetFiltersDefaultKey, presetFiltersKey, tableSortKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

type PresetFilter = {
  readonly name: string;
  readonly search: string;
};

const normalizeSearch = (value: string) => {
  const params = new URLSearchParams(value);

  params.sort();

  return params.toString();
};

export const PresetFiltersMenu = () => {
  const { t: translate } = useTranslation("common");
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [, setSorting] = useLocalStorage<SortingState>(tableSortKey(pathname), []);
  const [presetFilters, setPresetFilters] = useLocalStorage<Array<PresetFilter>>(
    presetFiltersKey(pathname),
    [],
  );
  const [defaultPresetName, setDefaultPresetName] = useLocalStorage<string | null>(
    presetFiltersDefaultKey(pathname),
    null,
  );
  const [name, setName] = useState("");
  const [open, setOpen] = useState(false);
  const [presetToDelete, setPresetToDelete] = useState<string | undefined>(undefined);

  const presetParams = new URLSearchParams(searchParams);

  presetParams.delete(SearchParamsKeys.OFFSET);
  presetParams.delete(SearchParamsKeys.CURSOR);
  // Pagination doesn't define a preset; any other URL param does. So a bare page has no params
  // left and can't be saved.
  const hasPresetToSave = [...presetParams].length > 0;
  const search = presetParams.toString();

  const duplicatePreset = presetFilters.find(
    (preset) => normalizeSearch(preset.search) === normalizeSearch(search),
  );

  const handleSave = () => {
    const trimmedName = name.trim();

    if (trimmedName === "" || !hasPresetToSave || duplicatePreset !== undefined) {
      return;
    }

    setPresetFilters((prev) =>
      prev.some((preset) => preset.name === trimmedName)
        ? prev.map((preset) => (preset.name === trimmedName ? { name: trimmedName, search } : preset))
        : [...prev, { name: trimmedName, search }],
    );
    setName("");
  };

  const applyPreset = (preset: PresetFilter) => {
    const params = new URLSearchParams(preset.search);

    // Unlike filters, the table (useTableURLState) also caches the sort in localStorage and falls
    // back to it when the URL has no sort. Mirror the preset's sort there too, or a preset without a
    // sort would inherit a stale one.
    setSorting(
      params
        .getAll(SearchParamsKeys.SORT)
        .map((sort) => ({ desc: sort.startsWith("-"), id: sort.replace("-", "") })),
    );
    setSearchParams(params);
    setOpen(false);
  };

  const deletePreset = (presetName: string) => {
    setPresetFilters((prev) => prev.filter((preset) => preset.name !== presetName));
    setDefaultPresetName((previous) => (previous === presetName ? null : previous));
  };

  const toggleDefault = (presetName: string) => {
    setDefaultPresetName((previous) => (previous === presetName ? null : presetName));
  };

  // Restore the default preset on a filterless landing. Keyed on pathname so the user's later
  // edits/clears don't re-trigger it and a deep link wins; only pagination is ignored, since any
  // other param makes the landing explicit and should be left alone.
  useEffect(() => {
    const target =
      defaultPresetName === null
        ? undefined
        : presetFilters.find((preset) => preset.name === defaultPresetName);
    const params = new URLSearchParams(searchParams);

    params.delete(SearchParamsKeys.OFFSET);
    params.delete(SearchParamsKeys.CURSOR);

    if (target !== undefined && [...params].length === 0) {
      applyPreset(target);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname]);

  let saveHint: string | undefined;

  if (!hasPresetToSave) {
    saveHint = translate("presetFilters.nothingToSave");
  } else if (duplicatePreset !== undefined) {
    saveHint = translate("presetFilters.duplicate", { name: duplicatePreset.name });
  }

  const infoContent = (
    <VStack alignItems="start" gap={2} maxW="260px">
      <Text fontSize="xs">{translate("presetFilters.info.save")}</Text>
      <Text fontSize="xs">{translate("presetFilters.info.default")}</Text>
      <Text fontSize="xs">{translate("presetFilters.info.storage")}</Text>
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
          <Button
            borderRadius="full"
            colorPalette="gray"
            data-testid="preset-filters-button"
            variant="outline"
          >
            <LuBookmark />
            {translate("presetFilters.title")}
          </Button>
        </Popover.Trigger>
        <Popover.Content width="xs">
          <Popover.Arrow />
          <VStack align="stretch" gap={2} p={4}>
            <HStack gap={1}>
              <Text fontSize="sm" fontWeight="medium">
                {translate("presetFilters.title")}
              </Text>
              <Tooltip content={infoContent}>
                <Box as="span" color="fg.muted" cursor="pointer">
                  <LuInfo />
                </Box>
              </Tooltip>
            </HStack>
            <HStack>
              <Input
                data-testid="preset-filter-name"
                onChange={(event) => setName(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handleSave();
                  }
                }}
                placeholder={translate("presetFilters.namePlaceholder")}
                size="sm"
                value={name}
              />
              <Tooltip content={saveHint ?? ""} disabled={saveHint === undefined}>
                <Box>
                  <Button
                    data-testid="preset-filter-save"
                    disabled={!hasPresetToSave || name.trim() === "" || duplicatePreset !== undefined}
                    onClick={handleSave}
                    size="sm"
                  >
                    {translate("presetFilters.save")}
                  </Button>
                </Box>
              </Tooltip>
            </HStack>
            {presetFilters.length === 0 ? (
              <Text color="fg.muted" fontSize="sm" py={1}>
                {translate("presetFilters.empty")}
              </Text>
            ) : (
              presetFilters.map((preset) => (
                <HStack gap={1} key={preset.name}>
                  <Button
                    flex="1"
                    justifyContent="flex-start"
                    minW={0}
                    onClick={() => applyPreset(preset)}
                    size="sm"
                    variant="ghost"
                  >
                    <Text truncate>{preset.name}</Text>
                  </Button>
                  <IconButton
                    label={
                      defaultPresetName === preset.name
                        ? translate("presetFilters.unsetDefault")
                        : translate("presetFilters.setDefault")
                    }
                    onClick={() => toggleDefault(preset.name)}
                    size="sm"
                    variant="ghost"
                  >
                    {defaultPresetName === preset.name ? <MdPushPin /> : <MdOutlinePushPin />}
                  </IconButton>
                  <IconButton
                    aria-label="Delete preset filter"
                    onClick={() => setPresetToDelete(preset.name)}
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
        onClose={() => setPresetToDelete(undefined)}
        onDelete={() => {
          if (presetToDelete !== undefined) {
            deletePreset(presetToDelete);
          }
          setPresetToDelete(undefined);
        }}
        open={presetToDelete !== undefined}
        resourceName={presetToDelete ?? ""}
        title={translate("presetFilters.deleteTitle")}
        warningText={translate("presetFilters.deleteWarning")}
      />
    </>
  );
};
