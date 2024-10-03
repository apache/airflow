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
import { Select, SingleValue, useChakraSelectProps } from "chakra-react-select";

import type { DatasetDependencies } from "src/api/useAssetDependencies";
import type { OnSelectProps } from "./types";

interface Props {
  datasetDependencies?: DatasetDependencies;
  selectedDagId?: string;
  selectedUri?: string;
  onSelectNode: (props: OnSelectProps) => void;
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
    if (node.value.class === "asset")
      datasetOptions.push({ value: node.id, label: node.value.label });
  });

  const onSelect = (option: SingleValue<Option>) => {
    let type = "";
    if (option) {
      if (option.value.startsWith("dataset:")) type = "dataset";
      else if (option.value.startsWith("dag:")) type = "dag";
      if (type === "dag") onSelectNode({ dagId: option.label });
      else if (type === "dataset") onSelectNode({ uri: option.label });
    }
  };

  let option;
  if (selectedUri) {
    option = { label: selectedUri, value: `dataset:${selectedUri}` };
  } else if (selectedDagId) {
    option = { label: selectedDagId, value: `dag:${selectedDagId}` };
  }

  const searchProps = useChakraSelectProps<Option, false>({
    selectedOptionStyle: "check",
    isDisabled: !datasetDependencies,
    value: option,
    onChange: onSelect,
    options: [
      { label: "DAGs", options: dagOptions },
      { label: "Datasets", options: datasetOptions },
    ],
    placeholder: "Search by DAG ID or Dataset URI",
    chakraStyles: {
      dropdownIndicator: (provided) => ({
        ...provided,
        bg: "transparent",
        px: 2,
        cursor: "inherit",
      }),
      indicatorSeparator: (provided) => ({
        ...provided,
        display: "none",
      }),
      menuList: (provided) => ({
        ...provided,
        py: 0,
      }),
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

  return <Select {...searchProps} />;
};

export default SearchBar;
