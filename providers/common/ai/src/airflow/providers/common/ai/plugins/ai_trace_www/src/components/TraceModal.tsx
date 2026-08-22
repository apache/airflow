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

import { Box, CloseButton, Spinner, Text } from "@chakra-ui/react";
import { type FC, type ReactNode, useEffect, useState } from "react";

import { ApiError, fetchTraceById } from "src/api";
import { TraceCard } from "src/components/TraceCard";
import type { TraceSummary } from "src/types";

// Centered overlay modal. Hand-rolled rather than Chakra's Dialog: the plugin
// renders inside Airflow's host page, and a plain fixed-position box avoids
// portal/z-index coupling with the host's own dialog layer.
const Overlay: FC<{ children: ReactNode; onClose: () => void }> = ({ children, onClose }) => {
  useEffect(() => {
    const onEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onEsc);
    return () => document.removeEventListener("keydown", onEsc);
  }, [onClose]);

  return (
    <Box
      alignItems="flex-start"
      bg="blackAlpha.600"
      bottom={0}
      display="flex"
      justifyContent="center"
      left={0}
      onClick={onClose}
      overflowY="auto"
      position="fixed"
      pt={12}
      right={0}
      top={0}
      zIndex={1400}
    >
      <Box
        bg="bg.panel"
        borderRadius="lg"
        borderWidth="1px"
        maxW="860px"
        mb={12}
        onClick={(e) => e.stopPropagation()}
        position="relative"
        shadow="lg"
        width="90%"
      >
        <CloseButton onClick={onClose} position="absolute" right={2} size="sm" top={2} zIndex={1} />
        {children}
      </Box>
    </Box>
  );
};

export const TraceModal: FC<{ onClose: () => void; traceId: string | null }> = ({ onClose, traceId }) => {
  const [summary, setSummary] = useState<TraceSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSummary(null);
    setError(null);
    if (!traceId) return;
    fetchTraceById(traceId)
      .then(setSummary)
      .catch((err: unknown) => {
        setError(err instanceof ApiError ? err.message : err instanceof Error ? err.message : String(err));
      });
  }, [traceId]);

  if (!traceId) return null;

  return (
    <Overlay onClose={onClose}>
      <Box p={2}>
        {error ? (
          <Text color="fg.error" fontSize="sm" p={4}>
            {error}
          </Text>
        ) : summary ? (
          <TraceCard summary={summary} />
        ) : (
          <Box p={8} textAlign="center">
            <Spinner size="sm" />
          </Box>
        )}
      </Box>
    </Overlay>
  );
};
