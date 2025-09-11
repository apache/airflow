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
import { Button, Group } from "@chakra-ui/react";
import { useEffect, useState } from "react";

import type { Option } from "src/utils/option";

type SegmentedControlProps = {
  readonly defaultValues?: Array<string>;
  readonly multiple?: boolean;
  readonly onChange?: (options: Array<string>) => void;
  readonly options: Array<Option>;
};

const SegmentedControl = ({ defaultValues, multiple = false, onChange, options }: SegmentedControlProps) => {
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(defaultValues ?? []);

  const onClick = (selected: string) => {
    if (multiple) {
      if (selectedOptions.includes(selected)) {
        setSelectedOptions((prevState) => prevState.filter((value) => value !== selected));
      } else {
        setSelectedOptions((prevState) => [...prevState, selected]);
      }
    } else {
      if (!selectedOptions.includes(selected)) {
        setSelectedOptions([selected]);
      }
    }
  };

  useEffect(() => onChange?.(selectedOptions), [onChange, selectedOptions]);

  return (
    <Group
      backgroundColor="bg.muted"
      borderColor="border.emphasized"
      borderRadius={8}
      borderWidth={1}
      colorPalette="brand"
      mb={3}
      p={1}
    >
      {options.map(({ disabled, label, value }: Option) => (
        <Button
          _hover={{ backgroundColor: "bg.emphasized" }}
          bg={selectedOptions.includes(value) ? "bg.panel" : undefined}
          disabled={disabled}
          key={value}
          onClick={() => onClick(value)}
          size="md"
          variant="ghost"
        >
          {label}
        </Button>
      ))}
    </Group>
  );
};

export default SegmentedControl;
