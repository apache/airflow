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
import type { ButtonProps, RecipeProps } from "@chakra-ui/react";
import { Button, FileUpload as ChakraFileUpload, Span, useRecipe } from "@chakra-ui/react";
import { forwardRef } from "react";
import { useTranslation } from "react-i18next";

type Assign<T, U> = Omit<T, keyof U> & U;

type FileInputProps = {
  readonly placeholder?: React.ReactNode;
} & Assign<ButtonProps, RecipeProps<"input">>;

export const FileInput = forwardRef<HTMLButtonElement, FileInputProps>((props, ref) => {
  const { t: translate } = useTranslation("components");
  const inputRecipe = useRecipe({ key: "input" });
  const [recipeProps, restProps] = inputRecipe.splitVariantProps(props);
  const { placeholder = "Select file(s)", ...rest } = restProps;

  return (
    <ChakraFileUpload.Trigger asChild>
      <Button py="0" ref={ref} unstyled {...rest} css={[inputRecipe(recipeProps), props.css]}>
        <ChakraFileUpload.Context>
          {({ acceptedFiles }) => {
            if (acceptedFiles.length === 1) {
              return <span>{acceptedFiles[0]?.name}</span>;
            }
            if (acceptedFiles.length > 1) {
              return <span>{translate("fileUpload.files_other", { count: acceptedFiles.length })}</span>;
            }

            return <Span color="fg.subtle">{placeholder}</Span>;
          }}
        </ChakraFileUpload.Context>
      </Button>
    </ChakraFileUpload.Trigger>
  );
});
