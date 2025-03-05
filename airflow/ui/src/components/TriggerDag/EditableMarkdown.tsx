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

import ReactMarkdown from "src/components/ReactMarkdown";

type EditableMarkdownProps = {
  readonly field: {
    onChange: (value: string) => void;
    value: string;
  };
  readonly placeholder: string;
};

const EditableMarkdown = ({ field, placeholder }: EditableMarkdownProps) => (
  <Editable.Root value={field.value}>
    <Editable.Preview
      _hover={{ backgroundColor: "transparent" }}
      alignItems="flex-start"
      as={VStack}
      width="100%"
    >
      {field.value ? (
        <ReactMarkdown>{field.value}</ReactMarkdown>
      ) : (
        <Text color="fg.subtle">{placeholder}</Text>
      )}
    </Editable.Preview>
    <Editable.Textarea
      minHeight="150px"
      onChange={(event: ChangeEvent<HTMLTextAreaElement>) => field.onChange(event.target.value)}
    />
  </Editable.Root>
);

export default EditableMarkdown;
