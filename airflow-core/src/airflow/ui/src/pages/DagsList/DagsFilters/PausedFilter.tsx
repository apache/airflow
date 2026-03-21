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

import { ButtonGroupToggle } from "src/components/ui";

type PausedValue = "all" | "false" | "true";

type Props = {
  readonly onChange: (value: PausedValue) => void;
  readonly value: PausedValue;
};

export const PausedFilter = ({ onChange, value }: Props) => {
  const { t: translate } = useTranslation("dags");

  const options = [
    { label: translate("filters.paused.all"), value: "all" as const },
    { label: translate("filters.paused.active"), value: "false" as const },
    { label: translate("filters.paused.paused"), value: "true" as const },
  ];

  return <ButtonGroupToggle<PausedValue> onChange={onChange} options={options} value={value} />;
};
