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
    if (isRequired(element) && (element.value === null || element.value === "")) {
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
  const [invalidFields, setInvalidFields] = useState<Set<string>>(new Set());

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
    setError(Object.keys(params).some((name) => invalidFields.has(name)) || newSectionError.size > 0);
  }, [params, flexibleFormDefaultSection, setError, invalidFields]);

  useEffect(() => {
    setDisabled(disabled ?? false);
  }, [disabled, setDisabled]);

  const onUpdate = (name: string, error?: unknown) => {
    setInvalidFields((previous) => {
      if (previous.has(name) === Boolean(error)) {
        return previous;
      }

      const next = new Set(previous);

      if (Boolean(error)) {
        next.add(name);
      } else {
        next.delete(name);
      }

      return next;
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
                    onUpdate={(_value, error) => onUpdate(name, error)}
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
