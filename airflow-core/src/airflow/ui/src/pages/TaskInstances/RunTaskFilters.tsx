/*!
 * Licensed to the Apache Software Foundation (ASF)...
 */
import { useMemo } from "react";
import { FilterBar, type FilterValue } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

export const RunTaskFilters = () => {
  // Keep to keys supported by FilterBar & TaskInstances API mapping
  const searchParamKeys = useMemo(
    (): Array<FilterableSearchParamsKeys> => [
      SearchParamsKeys.TASK_ID_PATTERN, // pill label "Task" (text)
      SearchParamsKeys.START_DATE,   // date from
      SearchParamsKeys.END_DATE,     // date to
    ],
    [],
  );

  const { filterConfigs, handleFiltersChange, searchParams } = useFiltersHandler(searchParamKeys);

  const initialValues = useMemo(() => {
    const values: Record<string, FilterValue> = {};

    filterConfigs.forEach((cfg) => {
      const raw = searchParams.get(cfg.key);

      if (raw !== null && raw !== "") {
        if (cfg.type === "number") {
          const num = Number(raw);

          values[cfg.key] = Number.isNaN(num) ? raw : num;
        } else {
          values[cfg.key] = raw;
        }
      }
    });

    return values;
  }, [searchParams, filterConfigs]);

  return <FilterBar configs={filterConfigs} initialValues={initialValues} onFiltersChange={handleFiltersChange} />;
};
