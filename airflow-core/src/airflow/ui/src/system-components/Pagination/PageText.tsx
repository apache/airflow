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
import { forwardRef } from "react";

import type { TextProps } from "@chakra-ui/react";
import { Text, usePaginationContext } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { formatNumber } from "src/utils/formatNumber";

type PageTextProps = {
  readonly format?: "compact" | "long" | "short";
} & TextProps;

export const PageText = forwardRef<HTMLParagraphElement, PageTextProps>((props, ref) => {
  const { format = "compact", ...rest } = props;
  const { i18n } = useTranslation();
  const { count, page, pageRange, pages } = usePaginationContext();

  const content = {
    compact: `${formatNumber(page, i18n.language)} of ${formatNumber(pages.length, i18n.language)}`,
    long: `${formatNumber(pageRange.start + 1, i18n.language)} - ${formatNumber(pageRange.end, i18n.language)} of ${formatNumber(count, i18n.language)}`,
    short: `${formatNumber(page, i18n.language)} / ${formatNumber(pages.length, i18n.language)}`,
  };

  return (
    <Text fontWeight="medium" ref={ref} {...rest}>
      {content[format]}
    </Text>
  );
});
