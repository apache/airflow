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
import {
  Text,
  Code,
  Divider,
  AccordionItem,
  AccordionPanel,
  Box,
} from "@chakra-ui/react";

import type { TaskInstanceAttributes } from "src/types";

import AccordionHeader from "src/components/AccordionHeader";
import sanitizeHtml from "sanitize-html";

interface Props {
  specialAttrsRendered?: TaskInstanceAttributes["specialAttrsRendered"];
}

const RenderedTemplates = ({ specialAttrsRendered }: Props) => {
  if (!specialAttrsRendered) return null;
  return (
    <AccordionItem>
      <AccordionHeader>Rendered Templates</AccordionHeader>
      <AccordionPanel>
        {Object.keys(specialAttrsRendered).map((key) => {
          if (!specialAttrsRendered[key]) return null;
          const renderedField = sanitizeHtml(specialAttrsRendered[key], {
            allowedAttributes: {
              "*": ["class"],
            },
          });

          return (
            <Box key={key} mt={3}>
              <Text as="strong">{key}</Text>
              <Divider my={2} />
              <Code
                fontSize="md"
                dangerouslySetInnerHTML={{
                  __html: renderedField,
                }}
              />
            </Box>
          );
        })}
      </AccordionPanel>
    </AccordionItem>
  );
};

export default RenderedTemplates;
