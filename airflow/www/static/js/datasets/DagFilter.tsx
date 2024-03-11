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

import React from "react";
import { HStack } from "@chakra-ui/react";
import { Size, useChakraSelectProps } from "chakra-react-select";

import type { DatasetDependencies } from "src/api/useDatasetDependencies";
import MultiSelect from "src/components/MultiSelect";
import InfoTooltip from "src/components/InfoTooltip";

interface Props {
  datasetDependencies?: DatasetDependencies;
  filteredDagIds: string[];
  onFilterDags: (dagIds: string[]) => void;
}

const transformArrayToMultiSelectOptions = (
  options: string[] | null
): { label: string; value: string }[] =>
  options === null
    ? []
    : options.map((option) => ({ label: option, value: option }));

const DagFilter = ({
  datasetDependencies,
  filteredDagIds,
  onFilterDags,
}: Props) => {
  const dagIds = (datasetDependencies?.nodes || [])
    .filter((node) => node.value.class === "dag")
    .map((dag) => dag.value.label);
  const options = dagIds.map((dagId) => ({ label: dagId, value: dagId }));

  const inputStyles: { backgroundColor: string; size: Size } = {
    backgroundColor: "white",
    size: "lg",
  };
  const multiSelectStyles = useChakraSelectProps({
    ...inputStyles,
    isMulti: true,
    tagVariant: "solid",
    hideSelectedOptions: false,
    isClearable: false,
    selectedOptionStyle: "check",
    chakraStyles: {
      container: (p) => ({
        ...p,
        width: "100%",
      }),
      placeholder: (p) => ({
        ...p,
        color: "gray.700",
        fontSize: "md",
      }),
      inputContainer: (p) => ({
        ...p,
        color: "gray.700",
        fontSize: "md",
      }),
      downChevron: (p) => ({
        ...p,
        fontSize: "lg",
      }),
      control: (p) => ({
        ...p,
        cursor: "pointer",
      }),
      option: (p) => ({
        ...p,
        transition: "background-color 0.2s",
        _hover: {
          bg: "gray.100",
        },
      }),
    },
  });

  return (
    <HStack width="100%">
      <MultiSelect
        {...multiSelectStyles}
        isDisabled={!datasetDependencies}
        value={transformArrayToMultiSelectOptions(filteredDagIds)}
        onChange={(dagOptions) => {
          if (
            Array.isArray(dagOptions) &&
            dagOptions.every((dagOption) => "value" in dagOption)
          ) {
            onFilterDags(dagOptions.map((option) => option.value));
          }
        }}
        options={options}
        placeholder="Filter graph by DAG ID"
      />
      <InfoTooltip label="Filter Datasets graph by anything that may be connected to one or more DAGs. Does not filter the datasets list." />
    </HStack>
  );
};

export default DagFilter;
