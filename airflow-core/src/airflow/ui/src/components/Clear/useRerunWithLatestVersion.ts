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
import { useEffect, useRef, useState } from "react";

import { useConfig } from "src/queries/useConfig";

type UseRunOnLatestVersionProps = {
  /** DAG-level rerun_with_latest_version from DAG details. */
  dagLevelConfig?: boolean | null;
};

type UseRunOnLatestVersionResult = {
  setValue: (value: boolean) => void;
  value: boolean;
};

/**
 * Resolves the default checkbox state for "Run on Latest Version".
 * Precedence: DAG-level > global config > false.
 *
 * Both dagLevelConfig and global config arrive asynchronously, so we
 * update the default once data loads — unless the user has already
 * manually toggled the checkbox.
 */
export const useRerunWithLatestVersion = ({
  dagLevelConfig,
}: UseRunOnLatestVersionProps): UseRunOnLatestVersionResult => {
  const globalConfigValue = useConfig("rerun_with_latest_version") as boolean | undefined;

  const resolvedDefault = dagLevelConfig ?? globalConfigValue ?? false;

  const [value, setValue] = useState(false);
  const userHasToggled = useRef(false);

  useEffect(() => {
    if (!userHasToggled.current && dagLevelConfig !== undefined) {
      setValue(resolvedDefault);
    }
  }, [dagLevelConfig, globalConfigValue, resolvedDefault]);

  const handleSetValue = (newValue: boolean) => {
    userHasToggled.current = true;
    setValue(newValue);
  };

  return { setValue: handleSetValue, value };
};
