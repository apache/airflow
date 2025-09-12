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
import { Box, VStack, Heading, Text, Button, Container, HStack, Code } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useNavigate, useRouteError, isRouteErrorResponse } from "react-router-dom";

import { AirflowPin } from "src/assets/AirflowPin";

export const ErrorPage = () => {
  const navigate = useNavigate();
  const error = useRouteError();
  const { t: translate } = useTranslation();

  let errorMessage = translate("error.defaultMessage");
  let statusCode = "";

  if (isRouteErrorResponse(error)) {
    statusCode = String(error.status);
    errorMessage =
      ((error as unknown as Error).message || (error as { statusText?: string }).statusText) ??
      translate("error.notFound");
  } else if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === "string") {
    errorMessage = error;
  }

  // This is vite's dev mode. We should switch to Airflow's dev mode as part of a UI config endpoint
  const isDev = import.meta.env.DEV;

  return (
    <Box alignItems="center" display="flex" justifyContent="center" pt={36} px={4}>
      <Container maxW="lg">
        <VStack gap={8} textAlign="center">
          <AirflowPin height="50px" width="50px" />

          <VStack gap={4}>
            <Heading>{statusCode || translate("error.title")}</Heading>
            <Text fontSize="lg">{errorMessage}</Text>
            {error instanceof Error && isDev ? (
              <Code borderRadius="md" fontSize="sm" p={3} width="full">
                {error.stack}
              </Code>
            ) : undefined}
          </VStack>

          <HStack gap={4}>
            <Button colorPalette="brand" onClick={() => navigate(-1)} size="lg">
              {translate("error.back")}
            </Button>
            <Button colorPalette="brand" onClick={() => navigate("/")} size="lg" variant="outline">
              {translate("error.home")}
            </Button>
          </HStack>
        </VStack>
      </Container>
    </Box>
  );
};
