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
import {
  Box,
  VStack,
  Heading,
  Text,
  Button,
  Container,
  HStack,
  Code,
  Icon,
} from "@chakra-ui/react";
import {
  useNavigate,
  useRouteError,
  isRouteErrorResponse,
} from "react-router-dom";

import { AirflowPin } from "src/assets/AirflowPin";

export const ErrorPage = () => {
  const navigate = useNavigate();
  const error = useRouteError();

  let errorMessage = "An unexpected error occurred";
  let statusCode = "";

  if (isRouteErrorResponse(error)) {
    statusCode = String(error.status);
    errorMessage =
      ((error as unknown as Error).message ||
        (error as { statusText?: string }).statusText) ??
      "Page Not Found";
  } else if (error instanceof Error) {
    errorMessage = error.message;
  } else if (typeof error === "string") {
    errorMessage = error;
  }

  // This is vite's dev mode. We should switch to Airflow's dev mode as part of a UI config endpoint
  const isDev = import.meta.env.DEV;

  return (
    <Box
      alignItems="center"
      display="flex"
      justifyContent="center"
      pt={36}
      px={4}
    >
      <Container maxW="lg">
        <VStack spacing={8} textAlign="center">
          <Icon as={AirflowPin} height="50px" width="50px" />

          <VStack spacing={4}>
            <Heading>{statusCode || "Error"}</Heading>
            <Text fontSize="lg">{errorMessage}</Text>
            {error instanceof Error && isDev ? (
              <Code borderRadius="md" fontSize="sm" p={3} width="full">
                {error.stack}
              </Code>
            ) : undefined}
          </VStack>

          <HStack spacing={4}>
            <Button colorScheme="blue" onClick={() => navigate(-1)} size="lg">
              Back
            </Button>
            <Button
              colorScheme="blue"
              onClick={() => navigate("/")}
              size="lg"
              variant="outline"
            >
              Home
            </Button>
          </HStack>
        </VStack>
      </Container>
    </Box>
  );
};
