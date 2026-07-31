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
import { MdOutlineStorage, MdSyncAlt } from "react-icons/md";
import { Outlet } from "react-router-dom";

import { NavTabs } from "src/layouts/Details/NavTabs";

/** Sub-nav tabs shared by the task-store and xcom routes. */
export const StorageLayout = () => {
  const { t: translate } = useTranslation("dag");

  return (
    <>
      <NavTabs
        tabs={[
          { icon: <MdOutlineStorage />, label: translate("tabs.taskStateStore"), value: "task-state-store" },
          { icon: <MdSyncAlt />, label: translate("tabs.xcom"), value: "xcom" },
        ]}
      />
      <Outlet />
    </>
  );
};
