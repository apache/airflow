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
import { Link, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { LimitedItemsList } from "src/components/LimitedItemsList";

const DEFAULT_OWNERS: Array<string> = [];
const MAX_OWNERS = 3;

export const DagOwners = ({
  ownerLinks,
  owners = DEFAULT_OWNERS,
}: {
  readonly ownerLinks?: Record<string, string> | null;
  readonly owners?: Array<string>;
}) => {
  const { t: translate } = useTranslation("dags");
  const items = owners.map((owner) => {
    const link = ownerLinks?.[owner];
    const hasOwnerLink = link !== undefined;

    return hasOwnerLink ? (
      <Link
        aria-label={translate("ownerLink", { owner })}
        asChild
        color="fg.info"
        href={link}
        key={owner}
        rel="noopener noreferrer"
        target="_blank"
      >
        {owner}
      </Link>
    ) : (
      <Text as="span" key={owner}>
        {owner}
      </Text>
    );
  });

  return <LimitedItemsList interactive items={items} maxItems={MAX_OWNERS} />;
};
