/* eslint-disable i18next/no-literal-string */

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
import { Card, Field, Heading, Input, Text, Textarea, VStack } from "@chakra-ui/react";
import { useState } from "react";

export const TextInputs = () => {
  const [inputValue, setInputValue] = useState("");
  const [textareaValue, setTextareaValue] = useState("");

  return (
    <Card.Root flex="1" minWidth="300px">
      <Card.Header>
        <Heading size="lg">Text Inputs</Heading>
        <Text color="fg.muted" fontSize="sm">
          Input fields and text areas
        </Text>
      </Card.Header>
      <Card.Body>
        <VStack align="stretch" gap={4}>
          <Field.Root>
            <Field.Label>Standard Input</Field.Label>
            <Input
              onChange={(event) => setInputValue(event.target.value)}
              placeholder="Enter some text..."
              value={inputValue}
            />
            <Field.HelperText>This is helper text for the input</Field.HelperText>
          </Field.Root>

          <Field.Root>
            <Field.Label>Password Input</Field.Label>
            <Input placeholder="Enter password..." type="password" />
          </Field.Root>

          <Field.Root>
            <Field.Label>Search Input</Field.Label>
            <Input placeholder="Search..." type="search" />
          </Field.Root>

          <Field.Root>
            <Field.Label>Disabled Input</Field.Label>
            <Input disabled placeholder="Disabled input" />
          </Field.Root>

          <Field.Root>
            <Field.Label>Error State Input</Field.Label>
            <Input data-invalid placeholder="Input with error" />
            <Field.ErrorText>This field has an error</Field.ErrorText>
          </Field.Root>

          <Field.Root>
            <Field.Label>Textarea</Field.Label>
            <Textarea
              onChange={(event) => setTextareaValue(event.target.value)}
              placeholder="Enter multiple lines of text..."
              rows={3}
              value={textareaValue}
            />
          </Field.Root>
        </VStack>
      </Card.Body>
    </Card.Root>
  );
};
