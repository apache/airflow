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
import { Stack, StackSeparator } from "@chakra-ui/react";

import { flexibleFormDefaultSection, type FlexibleFormProps } from ".";
import { Accordion, Alert } from "../ui";
import { Row } from "./Row";

export const FlexibleForm = ({ params }: FlexibleFormProps) => {
  const processedSections = new Map();

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
              <Accordion.ItemContent>
                <Stack separator={<StackSeparator />}>
                  <Alert
                    status="warning"
                    title="Population of changes in trigger form fields is not implemented yet. Please stay tuned for upcoming updates... and change the run conf in the 'Advanced Options' conf section below meanwhile."
                  />
                  {Object.entries(params)
                    .filter(
                      ([, param]) =>
                        param.schema.section === currentSection ||
                        (currentSection === flexibleFormDefaultSection && !Boolean(param.schema.section)),
                    )
                    .map(([name, param]) => (
                      <Row key={name} name={name} param={param} />
                    ))}
                </Stack>
              </Accordion.ItemContent>
            </Accordion.Item>
          );
        }
      })
    : undefined;
};

export default FlexibleForm;
