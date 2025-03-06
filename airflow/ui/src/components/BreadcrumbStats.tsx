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
import { HStack, Stat } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { LiaSlashSolid } from "react-icons/lia";
import { Link as RouterLink } from "react-router-dom";

import { Breadcrumb } from "src/components/ui";

type Links = Array<{ label: ReactNode | string; labelExtra?: ReactNode; title?: string; value?: string }>;

export const BreadcrumbStats = ({ links }: { readonly links: Links }) => (
  <Breadcrumb.Root separator={<LiaSlashSolid />}>
    {links.map((link, index) => (
      // eslint-disable-next-line react/no-array-index-key
      <Stat.Root gap={0} key={`${link.title}-${index}`}>
        <Stat.Label fontSize="xs" fontWeight="bold">
          {link.title}
        </Stat.Label>
        <Stat.ValueText fontSize="sm" fontWeight="normal" lineHeight={1.5}>
          {index === links.length - 1 ? (
            <Breadcrumb.CurrentLink>
              <HStack>{link.label}</HStack>
            </Breadcrumb.CurrentLink>
          ) : (
            <Breadcrumb.Link asChild color="fg.info">
              <HStack>
                {link.labelExtra}
                <RouterLink to={link.value ?? ""}>{link.label}</RouterLink>
              </HStack>
            </Breadcrumb.Link>
          )}
        </Stat.ValueText>
      </Stat.Root>
    ))}
  </Breadcrumb.Root>
);
