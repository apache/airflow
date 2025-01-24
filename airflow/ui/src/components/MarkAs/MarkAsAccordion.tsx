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
import { Editable, Text, VStack } from "@chakra-ui/react";
import type { ChangeEvent } from "react";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Accordion } from "src/components/ui";

type Props = {
  readonly note: DAGRunResponse["note"];
  readonly setNote: (value: string) => void;
};

const MarkAsAccordion = ({ note, setNote }: Props) => (
  <Accordion.Root collapsible defaultValue={["note"]} multiple={false} variant="enclosed">
    <Accordion.Item key="note" value="note">
      <Accordion.ItemTrigger>
        <Text fontWeight="bold">Note</Text>
      </Accordion.ItemTrigger>
      <Accordion.ItemContent>
        <Editable.Root
          onChange={(event: ChangeEvent<HTMLInputElement>) => setNote(event.target.value)}
          value={note ?? ""}
        >
          <Editable.Preview
            _hover={{ backgroundColor: "transparent" }}
            alignItems="flex-start"
            as={VStack}
            gap="0"
            height="200px"
            overflowY="auto"
            width="100%"
          >
            {Boolean(note) ? (
              <ReactMarkdown>{note}</ReactMarkdown>
            ) : (
              <Text color="gray" opacity={0.6}>
                Add a note...
              </Text>
            )}
          </Editable.Preview>
          <Editable.Textarea
            data-testid="notes-input"
            height="200px"
            overflowY="auto"
            placeholder="Add a note..."
            resize="none"
          />
        </Editable.Root>
      </Accordion.ItemContent>
    </Accordion.Item>
  </Accordion.Root>
);

export default MarkAsAccordion;
