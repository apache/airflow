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

import { Switch, Tooltip, type SwitchProps } from "src/system-components";

import { useTogglePauseWatcher } from "src/queries/useTogglePauseWatcher";

type Props = {
  readonly assetId: number;
  readonly isPaused: boolean;
  readonly watcherName: string;
} & SwitchProps;

export const TogglePauseWatcher = ({
  assetId,
  disabled,
  isPaused,
  watcherName,
  ...rest
}: Props) => {
  const { t: translate } = useTranslation(["common", "assets"]);
  const { isPending, mutate } = useTogglePauseWatcher();

  const handleCheckedChange = () => {
    mutate({
      assetId,
      requestBody: { is_paused: !isPaused },
      watcherName,
    });
  };

  const action = isPaused ? translate("common:unpause") : translate("common:pause");
  const label = `${action} ${watcherName}`;

  return (
    <Tooltip content={label}>
      <Switch
        aria-label={label}
        checked={!isPaused}
        colorPalette="success"
        disabled={isPending || disabled}
        onCheckedChange={handleCheckedChange}
        size="sm"
        {...rest}
      />
    </Tooltip>
  );
};
