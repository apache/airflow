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
import Editor, { type EditorProps } from "@monaco-editor/react";
import { useRef } from "react";

import { useColorMode } from "src/context/colorMode";

type JsonEditorProps = {
  readonly editable?: boolean;
  readonly height?: string;
  readonly id?: string;
  readonly name?: string;
  readonly onBlur?: () => void;
  readonly onChange?: (value: string) => void;
  readonly value?: string;
};

export const JsonEditor = ({
  editable = true,
  height = "200px",
  onBlur,
  onChange,
  value,
  ...rest
}: JsonEditorProps) => {
  const { colorMode } = useColorMode();
  const onBlurRef = useRef(onBlur);

  onBlurRef.current = onBlur;

  const options: EditorProps["options"] = {
    automaticLayout: true,
    folding: true,
    fontSize: 14,
    lineNumbers: "on",
    minimap: { enabled: false },
    readOnly: !editable,
    renderLineHighlight: "none",
    scrollBeyondLastLine: false,
  };

  const theme = colorMode === "dark" ? "vs-dark" : "vs-light";

  const handleChange = (val: string | undefined) => {
    onChange?.(val ?? "");
  };

  return (
    <div
      style={{
        border: "1px solid var(--chakra-colors-border-emphasized)",
        borderRadius: "8px",
        overflow: "hidden",
        width: "100%",
      }}
      {...rest}
    >
      <Editor
        height={height}
        language="json"
        onChange={handleChange}
        onMount={(editor) => {
          editor.onDidBlurEditorText(() => {
            onBlurRef.current?.();
          });
        }}
        options={options}
        theme={theme}
        value={value}
      />
    </div>
  );
};
