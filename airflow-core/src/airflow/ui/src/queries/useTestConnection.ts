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
import type { Dispatch, SetStateAction } from "react";
import { useTranslation } from "react-i18next";

import { useConnectionServiceTestConnection } from "openapi/queries";
import type { ConnectionTestResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

export const useTestConnection = (setConnected: Dispatch<SetStateAction<boolean | undefined>>) => {
  const { t: translate } = useTranslation("admin");

  const onSuccess = (res: ConnectionTestResponse) => {
    setConnected(res.status);
    if (res.status) {
      toaster.create({
        description: res.message,
        title: translate("connections.testSuccess.title"),
        type: "success",
      });
    } else {
      toaster.create({
        description: res.message,
        title: translate("connections.testError.title"),
        type: "error",
      });
    }
  };

  const onError = () => {
    setConnected(false);
  };

  return useConnectionServiceTestConnection({
    onError,
    onSuccess,
  });
};
