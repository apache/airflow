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
import { Icon, Stack, StackSeparator, Text } from "@chakra-ui/react";
import { useCallback, useEffect, useState } from "react";
import { MdError } from "react-icons/md";

import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import { Accordion } from "../ui";
import { Row } from "./Row";
import { isRequired } from "./isParamRequired";

export type FlexibleFormProps = {
  flexibleFormDefaultSection: string;
  initialParamsDict: { paramsDict: ParamsSpec };
  key?: string;
  setError: (error: boolean) => void;
  subHeader?: string;
};

export const FlexibleForm = ({
  flexibleFormDefaultSection,
  initialParamsDict,
  setError,
  subHeader,
}: FlexibleFormProps) => {
  const { paramsDict: params, setInitialParamDict, setParamsDict } = useParamStore();
  const processedSections = new Map();
  const [sectionError, setSectionError] = useState<Map<string, boolean>>(new Map());

  const recheckSection = useCallback(() => {
    sectionError.clear();
    Object.entries(params).forEach(([, element]) => {
      if (
        isRequired(element) &&
        (element.value === null || element.value === undefined || element.value === "")
      ) {
        sectionError.set(element.schema.section ?? flexibleFormDefaultSection, true);
        setSectionError(sectionError);
      }
    });
  }, [flexibleFormDefaultSection, params, sectionError]);

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
    recheckSection();
    if (sectionError.size === 0) {
      setError(false);
    } else {
      setError(true);
    }
  }, [params, setError, recheckSection, sectionError]);

  const onUpdate = (_value?: string, error?: unknown) => {
    recheckSection();
    if (!Boolean(error) && sectionError.size === 0) {
      setError(false);
    } else {
      setError(true);
    }
  };

  return Object.entries(params).some(([, param]) => typeof param.schema.section !== "string")
    ? Object.entries(params).map(([, secParam]) => {
        const currentSection = secParam.schema.section ?? flexibleFormDefaultSection;

        if (processedSections.has(currentSection)) {
          return undefined;
        } else {
          processedSections.set(currentSection, true);

          return (
            <Accordion.Item
              // We need to make the item content overflow visible for dropdowns to work, but directly applying the style does not work
              css={{
                "& > div:nth-of-type(1)": {
                  overflow: "visible",
                },
              }}
              key={currentSection}
              value={currentSection}
            >
              <Accordion.ItemTrigger cursor="button">
                <Text color={sectionError.get(currentSection) ? "fg.error" : undefined}>
                  {currentSection}
                </Text>
                {sectionError.get(currentSection) ? (
                  <Icon color="fg.error" margin="-1">
                    <MdError />
                  </Icon>
                ) : undefined}
              </Accordion.ItemTrigger>

              <Accordion.ItemContent pt={0}>
                <Accordion.ItemBody>
                  {Boolean(subHeader) ? (
                    <Text color="fg.muted" fontSize="xs" mb={2}>
                      {subHeader}
                    </Text>
                  ) : undefined}
                  <Stack separator={<StackSeparator />}>
                    {Object.entries(params)
                      .filter(
                        ([, param]) =>
                          param.schema.section === currentSection ||
                          (currentSection === flexibleFormDefaultSection && !Boolean(param.schema.section)),
                      )
                      .map(([name]) => (
                        <Row key={name} name={name} onUpdate={onUpdate} />
                      ))}
                  </Stack>
                </Accordion.ItemBody>
              </Accordion.ItemContent>
            </Accordion.Item>
          );
        }
      })
    : undefined;
};
