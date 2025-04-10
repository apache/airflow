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
import type { ButtonProps } from "@chakra-ui/react";
import { Pagination as ChakraPagination } from "@chakra-ui/react";
import { forwardRef } from "react";

import { paginationContext } from "./context";

type ButtonVariantMap = {
  current: ButtonProps["variant"];
  default: ButtonProps["variant"];
  ellipsis: ButtonProps["variant"];
};

type PaginationVariant = "outline" | "solid" | "subtle";

const [RootPropsProvider] = paginationContext;

export type PaginationRootProps = {
  readonly size?: ButtonProps["size"];
  readonly variant?: PaginationVariant;
} & Omit<ChakraPagination.RootProps, "type">;

const VARIANT_MAP: Record<PaginationVariant, ButtonVariantMap> = {
  outline: { current: "outline", default: "ghost", ellipsis: "plain" },
  solid: { current: "solid", default: "outline", ellipsis: "outline" },
  subtle: { current: "subtle", default: "ghost", ellipsis: "plain" },
};

export const Root = forwardRef<HTMLDivElement, PaginationRootProps>((props, ref) => {
  const { size = "sm", variant = "outline", ...rest } = props;

  return (
    <RootPropsProvider value={{ size, variantMap: VARIANT_MAP[variant] }}>
      <ChakraPagination.Root ref={ref} type="button" {...rest} />
    </RootPropsProvider>
  );
});
