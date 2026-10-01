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
import { Badge } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiPackage } from "react-icons/fi";

import type { DagBundleDetailResponse } from "openapi/requests/types.gen";

import { DagBundleVersion } from "src/components/DagBundleVersion";
import { HeaderCard } from "src/components/HeaderCard";
import { ImportErrorCount } from "src/components/ImportErrorCount";
import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";

import { useShowTeam } from "src/hooks/useShowTeam";

export const Header = ({ bundle }: { readonly bundle: DagBundleDetailResponse }) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  // Teams exist only in multi-team deployments, and a bundle need not belong to one; an
  // empty "Team" stat reads as a broken page.
  const showTeam = useShowTeam(bundle.team_name);

  const stats = [
    {
      label: translate("browse:dagBundles.detail.status"),
      value:
        bundle.active === true ? (
          <Badge colorPalette="success">{translate("browse:dagBundles.active")}</Badge>
        ) : (
          <Badge colorPalette="gray">{translate("browse:dagBundles.inactive")}</Badge>
        ),
    },
    {
      label: translate("browse:dagBundles.columns.version"),
      value: (
        <DagBundleVersion
          bundleUrl={bundle.bundle_url}
          lastRefreshed={bundle.last_refreshed}
          version={bundle.version}
        />
      ),
    },
    {
      label: translate("browse:dagBundles.columns.lastRefreshed"),
      value:
        bundle.last_refreshed === null ? (
          translate("browse:dagBundles.neverRefreshed")
        ) : (
          <Time datetime={bundle.last_refreshed} />
        ),
    },
    { label: translate("common:dag_other"), value: bundle.dag_count },
    {
      label: translate("browse:dagBundles.columns.importErrors"),
      value: <ImportErrorCount count={bundle.import_error_count} />,
    },
    ...(showTeam
      ? [
          {
            label: translate("common:dagDetails.team"),
            value: <TeamName teamName={bundle.team_name} />,
          },
        ]
      : []),
  ];

  return <HeaderCard icon={<FiPackage />} stats={stats} title={bundle.name} type="dagBundle" />;
};
