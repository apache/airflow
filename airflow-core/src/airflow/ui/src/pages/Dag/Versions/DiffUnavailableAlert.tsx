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
import { Alert, Code } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

type DiffUnavailableAlertProps = {
  readonly reason?: string | null;
};

export const DiffUnavailableAlert = ({ reason }: DiffUnavailableAlertProps) => {
  const { t: translate } = useTranslation("dag");

  return (
    <Alert.Root status="info" title={translate("versions.unavailable.title")}>
      <Alert.Description>
        {reason === null || reason === undefined ? (
          translate("versions.unavailable.withoutReason")
        ) : (
          <>
            {translate("versions.unavailable.description")}{" "}
            {/* A machine token, so it is shown as code rather than folded into a translated sentence. */}
            <Code fontSize="sm">{reason}</Code>
          </>
        )}
      </Alert.Description>
    </Alert.Root>
  );
};
