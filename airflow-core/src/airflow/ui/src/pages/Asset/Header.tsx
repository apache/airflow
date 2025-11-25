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
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";

import type { AssetResponse } from "openapi/requests/types.gen";
import { HeaderCard } from "src/components/HeaderCard";

import { DependencyPopover } from "../AssetsList/DependencyPopover";

export const Header = ({ asset }: { readonly asset?: AssetResponse }) => {
  const { t: translate } = useTranslation("assets");

  const stats = [
    { label: translate("group"), value: asset?.group },
    {
      label: translate("producingTasks"),
      value: <DependencyPopover dependencies={asset?.producing_tasks ?? []} type="Task" />,
    },
    {
      label: translate("scheduledDags"),
      value: <DependencyPopover dependencies={asset?.scheduled_dags ?? []} type="Dag" />,
    },
  ];

  return <HeaderCard icon={<FiDatabase />} stats={stats} title={asset?.name} />;
};
