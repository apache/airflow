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
import { Box, Flex, Separator, Text, VStack } from "@chakra-ui/react";
import { useRef, useState, type ReactNode, type RefObject } from "react";
import { useTranslation } from "react-i18next";

import { SearchAndFilter } from "src/components/SearchAndFilter";
import { SearchBar } from "src/components/SearchBar";
import { ButtonGroupToggle } from "src/components/ui";

import { FavoriteFilter } from "./FavoriteFilter";
import { FilterHubChip, type FilterHubChipData, type FilterHubFacet } from "./FilterHubChip";
import { OwnerFilter } from "./OwnerFilter";
import { RunStateSelect } from "./RunStateSelect";
import { TagFilter } from "./TagFilter";
import { TeamFilter } from "./TeamFilter";
import { TimetableTypeFilter } from "./TimetableTypeFilter";
import type { BooleanFilterValue, FilterHubProps } from "./types";

const runStates = ["failed", "queued", "running", "success"] as const;

const summarizeValues = (values: Array<string>) => {
  const [first, second] = values;

  if (values.length === 1) {
    return first ?? "";
  }
  if (values.length === 2) {
    return `${first}, ${second}`;
  }

  return `${first}, ${second} +${values.length - 2}`;
};

type FilterSectionProps = {
  readonly children: ReactNode;
  readonly facet?: FilterHubFacet;
  readonly highlighted: boolean;
  readonly title: string;
};

const FilterSection = ({ children, facet, highlighted, title }: FilterSectionProps) => (
  <Box
    as="fieldset"
    bg={highlighted ? "info.subtle" : undefined}
    border="0"
    borderRadius="md"
    data-filter-facet={facet}
    data-highlighted={highlighted || undefined}
    m={0}
    p={3}
  >
    <Text as="legend" fontSize="sm" fontWeight="semibold" mb={2} p={0}>
      {title}
    </Text>
    {children}
  </Box>
);

type FilterHubContentProps = {
  readonly contentRef: RefObject<HTMLDivElement | null>;
  readonly editingFacet: FilterHubFacet | undefined;
  readonly model: FilterHubProps["model"];
};

const FilterHubContent = ({ contentRef, editingFacet, model }: FilterHubContentProps) => {
  const { t: translate } = useTranslation(["dags", "hitl"]);

  return (
    <VStack align="stretch" gap={4} ref={contentRef}>
      <FilterSection
        highlighted={editingFacet === "lastRunState" || editingFacet === "activeRunState"}
        title={translate("dags:filters.statusGroup")}
      >
        <Flex flexWrap="wrap" gap={2}>
          <Box data-filter-facet="lastRunState">
            <RunStateSelect
              dataTestId="hub-last-run-state-filter"
              label={translate("dags:filters.lastRunState")}
              onChange={model.lastRunState.onChange}
              states={runStates}
              value={model.lastRunState.value}
            />
          </Box>
          <Box data-filter-facet="activeRunState">
            <RunStateSelect
              dataTestId="hub-any-run-state-filter"
              label={translate("dags:filters.anyRunState")}
              onChange={model.activeRunState.onChange}
              states={runStates}
              value={model.activeRunState.value}
            />
          </Box>
        </Flex>
      </FilterSection>
      <FilterSection
        highlighted={editingFacet === "paused" || editingFacet === "needsReview"}
        title={translate("dags:filters.availabilityGroup")}
      >
        <VStack align="stretch" gap={3}>
          <Box as="fieldset" border="0" data-filter-facet="paused" m={0} p={0}>
            <Text as="legend" color="fg.muted" fontSize="sm" mb={1} p={0}>
              {translate("dags:filters.pausedLabel")}
            </Text>
            <ButtonGroupToggle<BooleanFilterValue>
              attached={false}
              css={{ "& > button": { flex: "1 1 auto", minWidth: "fit-content" } }}
              flexWrap="wrap"
              gap={1}
              maxWidth="100%"
              onChange={model.paused.onChange}
              options={[
                { label: translate("dags:filters.paused.all"), value: "all" },
                { label: translate("dags:filters.paused.active"), value: "false" },
                { label: translate("dags:filters.paused.paused"), value: "true" },
              ]}
              value={model.paused.value}
              width="full"
            />
          </Box>
          <Box as="fieldset" border="0" data-filter-facet="needsReview" m={0} p={0}>
            <Text as="legend" color="fg.muted" fontSize="sm" mb={1} p={0}>
              {translate("dags:filters.needsReview")}
            </Text>
            <ButtonGroupToggle<BooleanFilterValue>
              attached={false}
              css={{ "& > button": { flex: "1 1 auto", minWidth: "fit-content" } }}
              flexWrap="wrap"
              gap={1}
              maxWidth="100%"
              onChange={model.needsReview.onChange}
              options={[
                {
                  dataTestId: "hub-needs-review-all",
                  label: translate("dags:filters.needsReviewAll"),
                  value: "all",
                },
                {
                  dataTestId: "hub-needs-review-filter",
                  label: translate("hitl:requiredAction_other"),
                  value: "true",
                },
              ]}
              value={model.needsReview.value}
              width="full"
            />
          </Box>
        </VStack>
      </FilterSection>
      <Separator />
      <FilterSection
        facet="timetableTypes"
        highlighted={editingFacet === "timetableTypes"}
        title={translate("common:dagDetails.schedule")}
      >
        <TimetableTypeFilter
          hasError={model.timetableTypes.hasError}
          hasNextPage={model.timetableTypes.hasNextPage}
          isLoading={model.timetableTypes.isLoading}
          onChange={model.timetableTypes.onChange}
          onInputChange={model.timetableTypes.onInputChange}
          onMenuScrollToBottom={model.timetableTypes.onMenuScrollToBottom}
          onMenuScrollToTop={model.timetableTypes.onMenuScrollToTop}
          onRetry={model.timetableTypes.onRetry}
          timetableTypes={model.timetableTypes.options}
          values={model.timetableTypes.values}
        />
      </FilterSection>
      <FilterSection
        highlighted={editingFacet === "owners" || editingFacet === "tags" || editingFacet === "teams"}
        title={translate("dags:filters.ownershipGroup")}
      >
        <Flex flexWrap="wrap" gap={3}>
          <Box data-filter-facet="owners">
            <OwnerFilter onChange={model.owners.onChange} values={model.owners.values} />
          </Box>
          <Box data-filter-facet="tags">
            <TagFilter
              hasError={model.tags.hasError}
              hasNextPage={model.tags.hasNextPage}
              isLoading={model.tags.isLoading}
              onMenuScrollToBottom={model.tags.onMenuScrollToBottom}
              onMenuScrollToTop={model.tags.onMenuScrollToTop}
              onRetry={model.tags.onRetry}
              onSelectTagsChange={model.tags.onChange}
              onTagModeChange={model.tags.onMatchModeChange}
              onUpdate={model.tags.onInputChange}
              selectedTags={model.tags.values}
              tagFilterMode={model.tags.matchMode}
              tags={model.tags.options}
            />
          </Box>
          {model.multiTeamEnabled ? (
            <Box data-filter-facet="teams">
              <TeamFilter onChange={model.teams.onChange} selectedTeams={model.teams.values} />
            </Box>
          ) : undefined}
        </Flex>
      </FilterSection>
      <FilterSection
        facet="favorite"
        highlighted={editingFacet === "favorite"}
        title={translate("dags:filters.personalGroup")}
      >
        <FavoriteFilter onChange={model.favorite.onChange} value={model.favorite.value} />
      </FilterSection>
    </VStack>
  );
};

export const FilterHub = ({ advancedSearch, model, onSearchChange, searchValue }: FilterHubProps) => {
  const { t: translate } = useTranslation(["dags", "common", "hitl"]);
  const [editingFacet, setEditingFacet] = useState<FilterHubFacet>();
  const [open, setOpen] = useState(false);
  const chipButtonRefs = useRef(new Map<FilterHubFacet, HTMLButtonElement>());
  const contentRef = useRef<HTMLDivElement>(null);
  const returnFocusFacetRef = useRef<FilterHubFacet | undefined>(undefined);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const facetLabels: Record<FilterHubFacet, string> = {
    activeRunState: translate("dags:filters.anyRunState"),
    favorite: translate("dags:filters.favorite.favorite"),
    lastRunState: translate("dags:filters.lastRunState"),
    needsReview: translate("dags:filters.needsReview"),
    owners: translate("common:owner"),
    paused: translate("dags:filters.pausedLabel"),
    tags: translate("common:dagDetails.tags"),
    teams: translate("dags:filters.teams"),
    timetableTypes: translate("dags:filters.timetableType"),
  };
  const chips: Array<FilterHubChipData> = [];
  const addChip = (
    facet: FilterHubFacet,
    summary: string | { readonly accessible: string; readonly visible: string },
    onRemove: () => void,
  ) => {
    const accessibleSummary = typeof summary === "string" ? summary : summary.accessible;
    const visibleSummary = typeof summary === "string" ? summary : summary.visible;

    chips.push({ accessibleSummary, facet, label: facetLabels[facet], onRemove, summary: visibleSummary });
  };

  if (model.lastRunState.value !== undefined) {
    addChip("lastRunState", translate(`common:states.${model.lastRunState.value}`), () =>
      model.lastRunState.onChange(undefined),
    );
  }
  if (model.activeRunState.value !== undefined) {
    addChip("activeRunState", translate(`common:states.${model.activeRunState.value}`), () =>
      model.activeRunState.onChange(undefined),
    );
  }
  if (model.needsReview.value !== "all") {
    addChip("needsReview", translate("hitl:requiredAction_other"), () => model.needsReview.onChange("all"));
  }
  if (model.paused.value !== "all") {
    addChip(
      "paused",
      translate(`dags:filters.paused.${model.paused.value === "true" ? "paused" : "active"}`),
      () => model.paused.onChange("all"),
    );
  }
  if (model.timetableTypes.values.length > 0) {
    addChip(
      "timetableTypes",
      {
        accessible: model.timetableTypes.values.join(", "),
        visible: summarizeValues(model.timetableTypes.values),
      },
      () => model.timetableTypes.onChange([]),
    );
  }
  if (model.tags.values.length > 0) {
    const tagMode = translate(`common:table.tagMode.${model.tags.matchMode}`);

    addChip(
      "tags",
      {
        accessible: `${model.tags.values.join(", ")} (${tagMode})`,
        visible: `${summarizeValues(model.tags.values)} (${tagMode})`,
      },
      () => model.tags.onChange([]),
    );
  }
  if (model.owners.values.length > 0) {
    addChip(
      "owners",
      { accessible: model.owners.values.join(", "), visible: summarizeValues(model.owners.values) },
      () => model.owners.onChange([]),
    );
  }
  if (model.multiTeamEnabled && model.teams.values.length > 0) {
    addChip(
      "teams",
      { accessible: model.teams.values.join(", "), visible: summarizeValues(model.teams.values) },
      () => model.teams.onChange([]),
    );
  }
  if (model.favorite.value !== "all") {
    addChip(
      "favorite",
      translate(`dags:filters.favorite.${model.favorite.value === "true" ? "favorite" : "unfavorite"}`),
      () => model.favorite.onChange("all"),
    );
  }

  const openFacet = (facet: FilterHubFacet) => {
    returnFocusFacetRef.current = facet;
    setEditingFacet(facet);
    setOpen(true);
  };
  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) {
      returnFocusFacetRef.current = undefined;
      setEditingFacet(undefined);
      setOpen(true);

      return;
    }

    setEditingFacet(undefined);
    setOpen(false);
    model.resetSuggestions();
  };
  const removeChip = (chip: FilterHubChipData, index: number) => {
    const focusFacet = chips[index + 1]?.facet ?? chips[index - 1]?.facet;

    (focusFacet === undefined ? triggerRef.current : chipButtonRefs.current.get(focusFacet))?.focus();
    chip.onRemove();
    if (editingFacet === chip.facet) {
      setEditingFacet(undefined);
    }
    if (returnFocusFacetRef.current === chip.facet) {
      returnFocusFacetRef.current = undefined;
    }
  };
  const getInitialFocusEl = () => {
    const content = contentRef.current;

    if (content === null) {
      return null;
    }

    const section =
      editingFacet === undefined
        ? content
        : content.querySelector<HTMLElement>(`[data-filter-facet="${editingFacet}"]`);

    return section?.querySelector<HTMLElement>("[role='combobox'], button, input, [tabindex]") ?? null;
  };
  const getFinalFocusEl = () => {
    const returnFacet = returnFocusFacetRef.current;

    returnFocusFacetRef.current = undefined;

    return returnFacet === undefined ? null : (chipButtonRefs.current.get(returnFacet) ?? null);
  };
  const currentEditingFacet =
    editingFacet !== undefined && chips.some(({ facet }) => facet === editingFacet)
      ? editingFacet
      : undefined;

  return (
    <SearchAndFilter
      activeFilterCount={chips.length}
      activeFilters={chips.map((chip, index) => (
        <FilterHubChip
          chip={chip}
          editButtonRef={(node) => {
            if (node === null) {
              chipButtonRefs.current.delete(chip.facet);
            } else {
              chipButtonRefs.current.set(chip.facet, node);
            }
          }}
          key={chip.facet}
          onEdit={() => openFacet(chip.facet)}
          onRemove={() => removeChip(chip, index)}
        />
      ))}
      activeFiltersTestId="hub-active-filters"
      finalFocusEl={getFinalFocusEl}
      initialFocusEl={getInitialFocusEl}
      labels={{
        activeFilterCount: (count) => translate("dags:filters.activeFilterCount", { count }),
        clearFilters: translate("dags:filters.clearFilters"),
        closeFilters: translate("dags:filters.closeFilters"),
        filterButton: translate("dags:filters.filterButton"),
        filterTitle: translate("dags:filters.filterDags"),
      }}
      onClearFilters={model.clearAll}
      onOpenChange={handleOpenChange}
      open={open}
      searchControl={
        <SearchBar
          advancedSearch={advancedSearch}
          ariaLabel={translate("dags:search.dags")}
          defaultValue={searchValue}
          onChange={onSearchChange}
          placeholder={translate("dags:search.dags")}
        />
      }
      triggerRef={triggerRef}
      triggerTestId="hub-filter-trigger"
    >
      <FilterHubContent contentRef={contentRef} editingFacet={currentEditingFacet} model={model} />
    </SearchAndFilter>
  );
};
