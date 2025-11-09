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
import { Box, VStack, Editable, Text } from "@chakra-ui/react";
import type { ChangeEvent } from "react";
import { useState, useRef } from "react";

import ReactMarkdown from "./ReactMarkdown";

const EditableMarkdownArea = ({
  mdContent,
  onBlur,
  placeholder,
  setMdContent,
}: {
  readonly mdContent?: string | null;
  readonly onBlur?: () => void;
  readonly placeholder?: string | null;
  readonly setMdContent: (value: string) => void;
}) => {
  const [currentValue, setCurrentValue] = useState(mdContent ?? "");
  const prevMdContentRef = useRef(mdContent);

  // Sync local state with prop changes
  if (mdContent !== prevMdContentRef.current) {
    setCurrentValue(mdContent ?? "");
    prevMdContentRef.current = mdContent;
  }

  return (
    <Box height="100%" p={4} width="100%">
      <Editable.Root
        height="100%"
        onBlur={onBlur}
        onChange={(event: ChangeEvent<HTMLInputElement>) => {
          const { value } = event.target;

          setCurrentValue(value);
          setMdContent(value);
        }}
        value={currentValue}
      >
        <Editable.Preview
          _hover={{ backgroundColor: "transparent" }}
          alignItems="flex-start"
          as={VStack}
          gap="0"
          height="100%"
          overflowY="auto"
          width="100%"
        >
          {Boolean(currentValue) ? (
            <ReactMarkdown>{currentValue}</ReactMarkdown>
          ) : (
            <Text color="fg.subtle">{placeholder}</Text>
          )}
        </Editable.Preview>
        <Editable.Textarea
          data-testid="markdown-input"
          height="100%"
          overflowY="auto"
          placeholder={placeholder ?? ""}
          resize="none"
        />
      </Editable.Root>
    </Box>
  );
};

export default EditableMarkdownArea;
