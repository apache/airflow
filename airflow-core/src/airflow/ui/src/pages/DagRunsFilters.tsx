import { VStack } from "@chakra-ui/react";

import { FilterBar } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

type DagRunsFiltersProps = {
  readonly dagId?: string;
};

export const DagRunsFilters = ({ dagId }: DagRunsFiltersProps) => {
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.RUN_ID_PATTERN,
    SearchParamsKeys.STATE,
    SearchParamsKeys.RUN_TYPE,

    // Correct keys
    SearchParamsKeys.START_DATE,
    SearchParamsKeys.END_DATE,

    SearchParamsKeys.RUN_AFTER_RANGE,
    SearchParamsKeys.DURATION_GTE,
    SearchParamsKeys.DURATION_LTE,
    SearchParamsKeys.CONF_CONTAINS,
    SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN,
    SearchParamsKeys.DAG_VERSION,
  ];

  if (dagId === undefined) {
    searchParamKeys.unshift(SearchParamsKeys.DAG_ID_PATTERN);
  }

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={handleFiltersChange}
      />
    </VStack>
  );
};
