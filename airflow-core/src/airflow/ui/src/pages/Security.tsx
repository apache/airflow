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
import { Box } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useAuthLinksServiceGetAuthMenus } from "openapi/queries";
import { ProgressBar } from "src/components/ui";

import { ErrorPage } from "./Error";

export const Security = () => {
  const { page } = useParams();

  const { data: authLinks, isLoading } = useAuthLinksServiceGetAuthMenus();

  const link = authLinks?.extra_menu_items.find((mi) => mi.text.toLowerCase().replace(" ", "-") === page);

  if (!link) {
    if (isLoading) {
      return (
        <Box flexGrow={1}>
          <ProgressBar />
        </Box>
      );
    }

    return <ErrorPage />;
  }

  // The following iframe sandbox setting is intentionally less restrictive.
  // This is considered safe because the framed content originates from the Auth manager,
  // which is part of the deployment of Airflow and trusted as per our security policy.
  // https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html
  const sandbox = "allow-scripts allow-same-origin allow-forms";

  return (
    <Box flexGrow={1} m={-3}>
      <iframe sandbox={sandbox} src={link.href} style={{ height: "100%", width: "100%" }} title={link.text} />
    </Box>
  );
};
