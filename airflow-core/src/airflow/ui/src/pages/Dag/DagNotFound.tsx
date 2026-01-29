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
import { Box, Button, Container, Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";

import { AirflowPin } from "src/assets/AirflowPin";

type DagNotFoundProps = {
  readonly dagId: string;
};

export const DagNotFound = ({ dagId }: DagNotFoundProps) => {
  const navigate = useNavigate();
  const { t: translate } = useTranslation("dag");

  return (
    <Box alignItems="center" display="flex" justifyContent="center" pt={36} px={4}>
      <Container maxW="lg">
        <VStack gap={8} textAlign="center">
          <AirflowPin height="50px" width="50px" />

          <VStack gap={4}>
            <Heading>404</Heading>
            <Text fontSize="lg">{translate("notFound.title")}</Text>
            <Text color="fg.muted">{translate("notFound.message", { dagId })}</Text>
          </VStack>

          <HStack gap={4}>
            <Button
              colorPalette="brand"
              onClick={() => {
                void Promise.resolve(navigate(-1));
              }}
              size="lg"
            >
              {translate("notFound.back")}
            </Button>
            <Button
              colorPalette="brand"
              onClick={() => {
                void Promise.resolve(navigate("/dags"));
              }}
              size="lg"
              variant="outline"
            >
              {translate("notFound.backToDags")}
            </Button>
          </HStack>
        </VStack>
      </Container>
    </Box>
  );
};
