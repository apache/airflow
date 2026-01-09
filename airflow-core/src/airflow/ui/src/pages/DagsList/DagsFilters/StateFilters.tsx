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

import { StateBadge } from "src/components/StateBadge";
import { ButtonGroupToggle, type ButtonGroupOption } from "src/components/ui/ButtonGroupToggle";

type StateValue = "all" | "failed" | "queued" | "running" | "success";

type Props = {
  readonly onChange: (value: StateValue) => void;
  readonly value: StateValue;
};

export const StateFilters = ({ onChange, value }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);

  const options: Array<ButtonGroupOption<StateValue>> = [
    { label: translate("dags:filters.paused.all"), value: "all" },
    {
      label: (
        <>
          <StateBadge state="failed" />
          {translate("common:states.failed")}
        </>
      ),
      value: "failed",
    },
    {
      label: (
        <>
          <StateBadge state="queued" />
          {translate("common:states.queued")}
        </>
      ),
      value: "queued",
    },
    {
      label: (
        <>
          <StateBadge state="running" />
          {translate("common:states.running")}
        </>
      ),
      value: "running",
    },
    {
      label: (
        <>
          <StateBadge state="success" />
          {translate("common:states.success")}
        </>
      ),
      value: "success",
    },
  ];

  return <ButtonGroupToggle<StateValue> onChange={onChange} options={options} value={value} />;
};
