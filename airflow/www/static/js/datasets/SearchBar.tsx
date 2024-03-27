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
import { Size, useChakraSelectProps } from "chakra-react-select";

import type { DatasetDependencies } from "src/api/useDatasetDependencies";
import MultiSelect from "src/components/MultiSelect";

interface Props {
  datasetDependencies?: DatasetDependencies;
  selectedDagId?: string;
  selectedUri?: string;
  onSelectNode: (id: string, type: string) => void;
}

interface Option {
  value: string;
  label: string;
}

const SearchBar = ({
  datasetDependencies,
  selectedDagId,
  selectedUri,
  onSelectNode,
}: Props) => {
  const dagOptions: Option[] = [];
  const datasetOptions: Option[] = [];
  (datasetDependencies?.nodes || []).forEach((node) => {
    if (node.value.class === "dag")
      dagOptions.push({ value: node.id, label: node.value.label });
    if (node.value.class === "dataset")
      datasetOptions.push({ value: node.id, label: node.value.label });
  });

  const inputStyles: { backgroundColor: string; size: Size } = {
    backgroundColor: "white",
    size: "lg",
  };
  const selectStyles = useChakraSelectProps({
    ...inputStyles,
    tagVariant: "solid",
    // hideSelectedOptions: false,
    // isClearable: false,
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

  const onSelect = ({ label, value }: Option) => {
    let type = "";
    if (value.startsWith("dataset:")) type = "dataset";
    else if (value.startsWith("dag:")) type = "dag";
    if (type) onSelectNode(label, type);
  };

  let option;
  if (selectedUri) {
    option = { label: selectedUri, value: `dataset:${selectedUri}` };
  } else if (selectedDagId) {
    option = { label: selectedDagId, value: `dag:${selectedDagId}` };
  }

  return (
    <MultiSelect
      {...selectStyles}
      isDisabled={!datasetDependencies}
      value={option}
      onChange={(e) => onSelect(e as Option)}
      options={[
        { label: "DAGs", options: dagOptions },
        { label: "Datasets", options: datasetOptions },
      ]}
      placeholder="Search by DAG ID or Dataset URI"
    />
  );
};

export default SearchBar;
