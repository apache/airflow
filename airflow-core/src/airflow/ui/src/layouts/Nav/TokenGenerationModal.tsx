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
import { Box, Button, Flex, HStack, Text } from "@chakra-ui/react";
import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle } from "react-icons/fi";

import { Dialog } from "src/components/ui";
import { ClipboardIconButton, ClipboardInput, ClipboardRoot } from "src/components/ui/Clipboard";

type TokenGenerationModalProps = {
  readonly isOpen: boolean;
  readonly onClose: () => void;
};

type TokenType = "api" | "cli";

const formatExpiration = (seconds: number): string => {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (hours > 0 && minutes > 0) {
    return `${String(hours)}h ${String(minutes)}m`;
  } else if (hours > 0) {
    return `${String(hours)}h`;
  }

  return `${String(minutes)}m`;
};

const TokenGenerationModal: React.FC<TokenGenerationModalProps> = ({ isOpen, onClose }) => {
  const { t: translate } = useTranslation();
  const [tokenType, setTokenType] = useState<TokenType>("api");
  const [generatedToken, setGeneratedToken] = useState<string | null>(null);
  const [expiresIn, setExpiresIn] = useState<number | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const handleClose = useCallback(() => {
    setGeneratedToken(null);
    setExpiresIn(null);
    setTokenType("api");
    onClose();
  }, [onClose]);

  const handleGenerate = useCallback(() => {
    setIsLoading(true);
    void fetch("/ui/auth/token", {
      body: JSON.stringify({ token_type: tokenType }),
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      method: "POST",
    })
      .then(async (response) => {
        if (response.ok) {
          const data = (await response.json()) as {
            access_token: string;
            expires_in_seconds: number;
            token_type: string;
          };

          setGeneratedToken(data.access_token);
          setExpiresIn(data.expires_in_seconds);
        }
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, [tokenType]);

  return (
    <Dialog.Root lazyMount onOpenChange={handleClose} open={isOpen} size="xl">
      <Dialog.Content backdrop>
        <Dialog.Header>{translate("tokenGeneration.title")}</Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          {generatedToken !== null && generatedToken !== "" ? (
            <Box>
              <Text fontWeight="semibold" mb={2}>
                {translate("tokenGeneration.tokenGenerated")}
              </Text>
              <ClipboardRoot value={generatedToken}>
                <Flex alignItems="center" gap={2}>
                  <ClipboardInput readOnly />
                  <ClipboardIconButton />
                </Flex>
              </ClipboardRoot>
              <HStack color="orange.500" gap={2} mt={3}>
                <FiAlertTriangle />
                <Text fontSize="sm">{translate("tokenGeneration.tokenShownOnce")}</Text>
              </HStack>
              {expiresIn !== null && expiresIn > 0 ? (
                <Text color="fg.muted" fontSize="sm" mt={2}>
                  {translate("tokenGeneration.tokenExpiresIn", {
                    duration: formatExpiration(expiresIn),
                  })}
                </Text>
              ) : undefined}
            </Box>
          ) : (
            <Box>
              <Text mb={3}>{translate("tokenGeneration.selectType")}</Text>
              <HStack gap={3} mb={4}>
                <Button
                  onClick={() => setTokenType("api")}
                  variant={tokenType === "api" ? "solid" : "outline"}
                >
                  {translate("tokenGeneration.apiToken")}
                </Button>
                <Button
                  onClick={() => setTokenType("cli")}
                  variant={tokenType === "cli" ? "solid" : "outline"}
                >
                  {translate("tokenGeneration.cliToken")}
                </Button>
              </HStack>
              <Button loading={isLoading} onClick={handleGenerate} width="full">
                {translate("tokenGeneration.generate")}
              </Button>
            </Box>
          )}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default TokenGenerationModal;
