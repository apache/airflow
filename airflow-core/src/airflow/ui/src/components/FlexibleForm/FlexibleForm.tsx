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
import { useEffect, useState, type ReactNode } from "react";

import { Heading, Icon, Stack, StackSeparator, Text } from "@chakra-ui/react";
import { MdError } from "react-icons/md";

import type { ParamsSpec, ParamSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import ReactMarkdown from "../ReactMarkdown";
import { Row } from "./Row";
import { isRequired } from "./isParamRequired";

const FlatSection = ({
  children,
  hasError,
  title,
}: {
  readonly children: ReactNode;
  readonly hasError: boolean;
  readonly title: string;
}) => (
  <Stack gap={2}>
    <Heading color={hasError ? "fg.error" : undefined} size="lg">
      {title}
      {hasError ? (
        <Icon color="fg.error" margin="-1" ml={1}>
          <MdError />
        </Icon>
      ) : undefined}
    </Heading>
    {children}
  </Stack>
);

const computeSectionErrors = (
  params: Record<string, ParamSpec>,
  defaultSection: string,
): Map<string, boolean> => {
  const errors = new Map<string, boolean>();

  Object.values(params).forEach((element) => {
    if (
      isRequired(element) &&
      (element.value === null || element.value === undefined || element.value === "")
    ) {
      errors.set(element.schema.section ?? defaultSection, true);
    }
  });

  return errors;
};

export type FlexibleFormProps = {
  readonly disabled?: boolean;
  readonly flexFormDescription?: string;
  readonly flexibleFormDefaultSection: string;
  readonly initialParamsDict: { paramsDict: ParamsSpec };
  readonly isHITL?: boolean;
  readonly key?: string;
  readonly namespace?: string;
  readonly setError: (error: boolean) => void;
  readonly subHeader?: string;
};

export const FlexibleForm = ({
  disabled,
  flexFormDescription,
  flexibleFormDefaultSection,
  initialParamsDict,
  isHITL,
  namespace = "default",
  setError,
  subHeader,
}: FlexibleFormProps) => {
  const { paramsDict: params, setDisabled, setInitialParamDict, setParamsDict } = useParamStore(namespace);
  const processedSections = new Map();
  const [sectionError, setSectionError] = useState<Map<string, boolean>>(new Map());
  const [validationErrors, setValidationErrors] = useState<Map<string, unknown>>(new Map());

  useEffect(() => {
    // Initialize paramsDict and initialParamDict when modal opens
    if (Object.keys(initialParamsDict.paramsDict).length > 0 && Object.keys(params).length === 0) {
      const paramsCopy = structuredClone(initialParamsDict.paramsDict);

      setParamsDict(paramsCopy);
      setInitialParamDict(initialParamsDict.paramsDict);
    }
  }, [initialParamsDict, params, setParamsDict, setInitialParamDict]);

  useEffect(
    () => () => {
      // Clear paramsDict and initialParamDict when the component is unmounted or modal closes
      setParamsDict({});
      setInitialParamDict({});
    },
    [setParamsDict, setInitialParamDict],
  );

  useEffect(() => {
    const newSectionError = computeSectionErrors(params, flexibleFormDefaultSection);

    setSectionError(newSectionError);
    setError(validationErrors.size > 0 || newSectionError.size > 0);
  }, [params, flexibleFormDefaultSection, setError, validationErrors]);

  useEffect(() => {
    setDisabled(disabled ?? false);
  }, [disabled, setDisabled]);

  // Validation errors are tracked per param: an error raised by one param must not be cleared
  // when another param is edited, otherwise the form would be considered valid again while the
  // offending value is still in place.
  const onUpdate = (name: string, _value?: string, error?: unknown) => {
    setValidationErrors((previousErrors) => {
      if (!Boolean(error) && !previousErrors.has(name)) {
        return previousErrors;
      }

      const nextErrors = new Map(previousErrors);

      if (Boolean(error)) {
        nextErrors.set(name, error);
      } else {
        nextErrors.delete(name);
      }

      return nextErrors;
    });
  };

  return Object.keys(params).length > 0 ? (
    <>
      {Object.entries(params).map(([, secParam]) => {
        const currentSection = secParam.schema.section ?? flexibleFormDefaultSection;

        if (processedSections.has(currentSection)) {
          return undefined;
        }
        processedSections.set(currentSection, true);

        return (
          <FlatSection
            hasError={Boolean(sectionError.get(currentSection))}
            key={currentSection}
            title={currentSection}
          >
            {Boolean(subHeader) ? (
              <Text color="fg.muted" fontSize="xs">
                {subHeader}
              </Text>
            ) : undefined}
            <Stack separator={<StackSeparator py={2} />}>
              {Boolean(flexFormDescription) ? (
                <ReactMarkdown>{flexFormDescription}</ReactMarkdown>
              ) : undefined}
              {Object.entries(params)
                .filter(
                  ([, param]) =>
                    param.schema.section === currentSection ||
                    (currentSection === flexibleFormDefaultSection && !Boolean(param.schema.section)),
                )
                .map(([name]) => (
                  <Row
                    key={name}
                    name={name}
                    namespace={namespace}
                    onUpdate={(value, error) => onUpdate(name, value, error)}
                  />
                ))}
            </Stack>
          </FlatSection>
        );
      })}
    </>
  ) : isHITL ? (
    <FlatSection
      hasError={Boolean(sectionError.get(flexibleFormDefaultSection))}
      title={flexibleFormDefaultSection}
    >
      {Boolean(flexFormDescription) ? <ReactMarkdown>{flexFormDescription}</ReactMarkdown> : undefined}
    </FlatSection>
  ) : undefined;
};
