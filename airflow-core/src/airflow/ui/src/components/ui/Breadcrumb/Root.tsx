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
import { Breadcrumb, type SystemStyleObject } from "@chakra-ui/react";
import React from "react";

export type BreadcrumbRootProps = {
  readonly separator?: React.ReactNode;
  readonly separatorGap?: SystemStyleObject["gap"];
} & Breadcrumb.RootProps;

export const Root = React.forwardRef<HTMLDivElement, BreadcrumbRootProps>((props, ref) => {
  const { children, separator, separatorGap, ...rest } = props;

  const validChildren = React.Children.toArray(children).filter(React.isValidElement);

  return (
    <Breadcrumb.Root ref={ref} {...rest}>
      <Breadcrumb.List gap={separatorGap}>
        {validChildren.map((child, index) => {
          const last = index === validChildren.length - 1;

          return (
            // eslint-disable-next-line react/no-array-index-key
            <React.Fragment key={index}>
              <Breadcrumb.Item>{child}</Breadcrumb.Item>
              {!last && <Breadcrumb.Separator>{separator}</Breadcrumb.Separator>}
            </React.Fragment>
          );
        })}
      </Breadcrumb.List>
    </Breadcrumb.Root>
  );
});
