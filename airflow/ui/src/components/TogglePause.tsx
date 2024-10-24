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
import { Switch } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import {
  useDagServiceGetDagsKey,
  useDagServicePatchDag,
} from "openapi/queries";

type Props = {
  readonly dagId: string;
  readonly isPaused: boolean;
};

export const TogglePause = ({ dagId, isPaused }: Props) => {
  const queryClient = useQueryClient();

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useDagServiceGetDagsKey],
    });
  };

  const { mutate } = useDagServicePatchDag({
    onSuccess,
  });

  const onChange = useCallback(() => {
    mutate({
      dagId,
      requestBody: {
        is_paused: !isPaused,
      },
    });
  }, [dagId, isPaused, mutate]);

  return <Switch isChecked={!isPaused} onChange={onChange} size="sm" />;
};
