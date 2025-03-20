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
import { Box, Stack, StackSeparator } from "@chakra-ui/react";
import { useEffect } from "react";

import type { ParamsSpec } from "src/queries/useDagParams";
import { useParamStore } from "src/queries/useParamStore";

import { Accordion } from "../ui";
import { Row } from "./Row";

export type FlexibleFormProps = {
  flexibleFormDefaultSection: string;
  initialParamsDict: { paramsDict: ParamsSpec };
  key?: string;
};

export const FlexibleForm = ({ flexibleFormDefaultSection, initialParamsDict }: FlexibleFormProps) => {
  const { paramsDict: params, setinitialParamDict, setParamsDict } = useParamStore();
  const processedSections = new Map();

  useEffect(() => {
    // Initialize paramsDict and initialParamDict when modal opens
    if (Object.keys(initialParamsDict.paramsDict).length > 0 && Object.keys(params).length === 0) {
      const paramsCopy = structuredClone(initialParamsDict.paramsDict);

      setParamsDict(paramsCopy);
      setinitialParamDict(initialParamsDict.paramsDict);
    }
  }, [initialParamsDict, params, setParamsDict, setinitialParamDict]);

  useEffect(
    () => () => {
      // Clear paramsDict and initialParamDict when the component is unmounted or modal closes
      setParamsDict({});
      setinitialParamDict({});
    },
    [setParamsDict, setinitialParamDict],
  );

  return Object.entries(params).some(([, param]) => typeof param.schema.section !== "string")
    ? Object.entries(params).map(([, secParam]) => {
        const currentSection = secParam.schema.section ?? flexibleFormDefaultSection;

        if (processedSections.has(currentSection)) {
          return undefined;
        } else {
          processedSections.set(currentSection, true);

          return (
            <Accordion.Item key={currentSection} value={currentSection}>
              <Accordion.ItemTrigger cursor="button">{currentSection}</Accordion.ItemTrigger>
              <Accordion.ItemContent paddingTop={0}>
                <Box p={5}>
                  <Stack separator={<StackSeparator />}>
                    {Object.entries(params)
                      .filter(
                        ([, param]) =>
                          param.schema.section === currentSection ||
                          (currentSection === flexibleFormDefaultSection && !Boolean(param.schema.section)),
                      )
                      .map(([name]) => (
                        <Row key={name} name={name} />
                      ))}
                  </Stack>
                </Box>
              </Accordion.ItemContent>
            </Accordion.Item>
          );
        }
      })
    : undefined;
};
