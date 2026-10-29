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
import { FiBarChart } from "react-icons/fi";

import type { MenuItem } from "openapi/requests/types.gen";

import { NavButton } from "./NavButton";

export const RunsButton = ({ authorizedMenuItems }: { readonly authorizedMenuItems: Array<MenuItem> }) => {
  const { t: translate } = useTranslation();
  // Dag runs have no menu item of their own: they follow the Dags permission.
  const isAuthorized = authorizedMenuItems.includes("Dags");

  // Without a destination NavButton renders a real <button>, which can be disabled. A disabled
  // anchor would stay clickable.
  return (
    <NavButton
      data-testid="nav-runs-link"
      disabled={!isAuthorized}
      // The run bars, matching the Dag page's own Runs tab.
      icon={FiBarChart}
      title={translate("runs")}
      to={isAuthorized ? "/dag_runs" : undefined}
    />
  );
};
