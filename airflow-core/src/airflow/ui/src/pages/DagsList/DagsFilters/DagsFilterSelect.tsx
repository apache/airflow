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
import { Button, Field, HStack, Text } from "@chakra-ui/react";
import { Select as ReactSelect } from "chakra-react-select";
import { useTranslation } from "react-i18next";

type FilterOption = {
  label: string;
  value: string;
};

type Props = {
  readonly ariaLabel: string;
  readonly hasError?: boolean;
  readonly hasNextPage?: boolean;
  readonly isLoading?: boolean;
  readonly noOptionsMessage: string;
  readonly onChange: (values: Array<string>) => void;
  readonly onInputChange?: (value: string) => void;
  readonly onMenuScrollToBottom?: () => void;
  readonly onMenuScrollToTop?: () => void;
  readonly onRetry?: () => void;
  readonly options: Array<string>;
  readonly placeholder: string;
  readonly values: Array<string>;
};

export const DagsFilterSelect = ({
  ariaLabel,
  hasError = false,
  hasNextPage = false,
  isLoading = false,
  noOptionsMessage,
  onChange,
  onInputChange,
  onMenuScrollToBottom,
  onMenuScrollToTop,
  onRetry,
  options,
  placeholder,
  values,
}: Props) => {
  const { t: translate } = useTranslation("dags");

  return (
    <Field.Root invalid={hasError}>
      <ReactSelect<FilterOption, true>
        aria-label={ariaLabel}
        chakraStyles={{
          clearIndicator: (provided) => ({
            ...provided,
            color: "gray.fg",
          }),
          container: (provided) => ({
            ...provided,
            width: "100%",
          }),
          control: (provided) => ({
            ...provided,
            colorPalette: "brand",
          }),
          menu: (provided) => ({
            ...provided,
            zIndex: 2,
          }),
        }}
        isClearable
        isLoading={isLoading}
        isMulti
        loadingMessage={() => translate("filters.suggestionsLoading")}
        noOptionsMessage={() => (hasError ? translate("filters.suggestionsError") : noOptionsMessage)}
        onChange={(selected) => onChange(selected.map(({ value }) => value))}
        onInputChange={onInputChange}
        onMenuScrollToBottom={onMenuScrollToBottom}
        onMenuScrollToTop={onMenuScrollToTop}
        options={options.map((option) => ({ label: option, value: option }))}
        placeholder={placeholder}
        value={values.map((value) => ({ label: value, value }))}
      />
      {hasError ? (
        <HStack role="alert">
          <Field.ErrorText>{translate("filters.suggestionsError")}</Field.ErrorText>
          {onRetry === undefined ? undefined : (
            <Button onClick={onRetry} size="xs" variant="outline">
              {translate("filters.retrySuggestions")}
            </Button>
          )}
        </HStack>
      ) : isLoading ? (
        <Text aria-live="polite" as="output" fontSize="xs">
          {translate("filters.suggestionsLoading")}
        </Text>
      ) : hasNextPage ? (
        <Text aria-live="polite" as="output" fontSize="xs">
          {translate("filters.moreSuggestionsAvailable")}
        </Text>
      ) : undefined}
    </Field.Root>
  );
};
