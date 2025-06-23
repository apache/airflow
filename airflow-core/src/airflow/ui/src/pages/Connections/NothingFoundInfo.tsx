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
import { Box, Text, Link, Stack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useVersionServiceGetVersion } from "openapi/queries";

export const NothingFoundInfo = () => {
  const { t: translate } = useTranslation("admin");
  const { data } = useVersionServiceGetVersion();
  const docsLink = `https://airflow.apache.org/docs/apache-airflow/${data?.version}/howto/connection.html#visibility-in-ui-and-cli`;

  return (
    <Box textAlign="center">
      <Stack>
        <Text fontSize="2xl" fontWeight="bold">
          {translate("connections.nothingFound.title")}
        </Text>
        <Text>{translate("connections.nothingFound.description")}</Text>
        <Text>
          {translate("connections.nothingFound.learnMore")}{" "}
          <Link href={docsLink} target="blank">
            {translate("connections.nothingFound.documentationLink")}
          </Link>
        </Text>
      </Stack>
    </Box>
  );
};
