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

import type { MenuItem } from "openapi/requests/types.gen";

import { DagIcon } from "src/assets/DagIcon";

import { NavButton } from "./NavButton";

export const DagsButton = ({ authorizedMenuItems }: { readonly authorizedMenuItems: Array<MenuItem> }) => {
  const { t: translate } = useTranslation();
  const isAuthorized = authorizedMenuItems.includes("Dags");

  // Without a destination NavButton renders a real <button>, which can be disabled. A disabled
  // anchor would stay clickable.
  return (
    <NavButton
      data-testid="nav-dags-link"
      disabled={!isAuthorized}
      icon={DagIcon}
      title={translate("nav.dags")}
      to={isAuthorized ? "/dags" : undefined}
    />
  );
};
