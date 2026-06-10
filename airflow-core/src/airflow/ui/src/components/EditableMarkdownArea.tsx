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
import type { Components } from "react-markdown";

import ReactMarkdown from "./ReactMarkdown";

const EditableMarkdownArea = ({
  autoSize = false,
  components,
  mdContent,
  onBlur,
  onFocus,
  padding = 4,
  placeholder,
  setMdContent,
}: {
  readonly autoSize?: boolean;
  readonly components?: Partial<Components>;
  readonly mdContent?: string | null;
  readonly onBlur?: () => void;
  readonly onFocus?: () => void;
  readonly padding?: number;
  readonly placeholder?: string | null;
  readonly setMdContent: (value: string) => void;
}) => {
  const [currentValue, setCurrentValue] = useState(mdContent ?? "");
  const prevMdContentRef = useRef(mdContent);
  const textareaRef = useRef<HTMLInputElement>(null);

  // Sync local state with prop changes (e.g. revert-on-error from parent)
  if (mdContent !== prevMdContentRef.current) {
    setCurrentValue(mdContent ?? "");
    prevMdContentRef.current = mdContent;
  }

  const resizeTextarea = () => {
    const el = textareaRef.current;

    if (autoSize && el !== null) {
      el.style.height = "auto";
      el.style.height = `${el.scrollHeight}px`;
    }
  };

  return (
    <Box height={autoSize ? undefined : "100%"} p={padding} width="100%">
      <Editable.Root
        height={autoSize ? undefined : "100%"}
        onBlur={onBlur}
        onChange={(event: ChangeEvent<HTMLInputElement>) => {
          const { value } = event.target;

          setCurrentValue(value);
          setMdContent(value);
          resizeTextarea();
        }}
        value={currentValue}
      >
        <Editable.Preview
          _hover={{ backgroundColor: "transparent" }}
          alignItems="flex-start"
          as={VStack}
          gap="0"
          height={autoSize ? undefined : "100%"}
          overflowY={autoSize ? undefined : "auto"}
          width="100%"
        >
          {Boolean(currentValue) ? (
            <ReactMarkdown components={components}>{currentValue}</ReactMarkdown>
          ) : (
            <Text color="fg.subtle">{placeholder}</Text>
          )}
        </Editable.Preview>
        <Editable.Textarea
          _focus={{ borderColor: "brand.focusRing" }}
          borderColor="transparent"
          borderRadius="sm"
          borderWidth="1px"
          data-testid="markdown-input"
          height={autoSize ? undefined : "100%"}
          onFocus={() => {
            // Resize on focus so the textarea fits existing content when first opened
            resizeTextarea();
            onFocus?.();
          }}
          overflow={autoSize ? "hidden" : "auto"}
          placeholder={placeholder ?? ""}
          ref={textareaRef}
          resize="none"
        />
      </Editable.Root>
    </Box>
  );
};

export default EditableMarkdownArea;
