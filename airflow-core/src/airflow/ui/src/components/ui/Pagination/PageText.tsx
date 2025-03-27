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
import type { TextProps } from "@chakra-ui/react";
import { Text, usePaginationContext } from "@chakra-ui/react";
import { forwardRef, useMemo } from "react";

type PageTextProps = {
  readonly format?: "compact" | "long" | "short";
} & TextProps;

export const PageText = forwardRef<HTMLParagraphElement, PageTextProps>((props, ref) => {
  const { format = "compact", ...rest } = props;
  const { count, page, pageRange, pages } = usePaginationContext();
  const content = useMemo(() => {
    if (format === "short") {
      return `${page} / ${pages.length}`;
    }
    if (format === "compact") {
      return `${page} of ${pages.length}`;
    }

    return `${pageRange.start + 1} - ${pageRange.end} of ${count}`;
  }, [format, page, pages.length, pageRange, count]);

  return (
    <Text fontWeight="medium" ref={ref} {...rest}>
      {content}
    </Text>
  );
});
