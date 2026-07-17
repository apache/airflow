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
  Badge,
  Box,
  Button,
  Flex,
  HStack,
  Heading,
  Spinner,
  Text,
  Textarea,
  VStack,
} from "@chakra-ui/react";
import {
  type FC,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

import { MessageBubble } from "src/components/MessageBubble";
import { NoSession } from "src/components/NoSession";
import { useSession } from "src/hooks/useSession";
import { isTerminalStatus } from "src/types/feedback";
import { toaster } from "src/toaster";

interface ChatPageProps {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex: number;
}

type ConfirmAction = "approve" | "reject" | null;

const STATUS_BADGE: Record<
  string,
  { colorPalette: "green" | "red" | "yellow" | "blue"; label: string }
> = {
  pending_review: { colorPalette: "yellow", label: "Pending Review" },
  approved: { colorPalette: "green", label: "Approved" },
  rejected: { colorPalette: "red", label: "Rejected" },
  changes_requested: { colorPalette: "blue", label: "Regenerating..." },
  max_iterations_exceeded: { colorPalette: "red", label: "Max iterations exceeded" },
  timeout_exceeded: { colorPalette: "red", label: "Timeout exceeded" },
};

export const ChatPage: FC<ChatPageProps> = ({ dagId, runId, taskId, mapIndex }) => {
  const { session, loading, error, sendFeedback, approve, reject } = useSession(
    dagId,
    runId,
    taskId,
    mapIndex,
  );

  const [feedbackText, setFeedbackText] = useState("");
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>(null);
  const [isSending, setIsSending] = useState(false);
  const chatRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const prevConvLenRef = useRef(0);

  useEffect(() => {
    if (!session?.conversation || !chatRef.current) return;
    const len = session.conversation.length;
    if (len > prevConvLenRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
    prevConvLenRef.current = len;
  }, [session?.conversation]);

  const autoResize = useCallback(() => {
    const ta = textareaRef.current;
    if (ta) {
      ta.style.height = "auto";
      ta.style.height = `${ta.scrollHeight}px`;
    }
  }, []);

  const handleSend = useCallback(async () => {
    const text = feedbackText.trim();
    if (!text || isSending) return;
    setIsSending(true);
    try {
      await sendFeedback(text);
      setFeedbackText("");
      toaster.create({ title: "Feedback sent", type: "success", duration: 4000 });
    } catch (err) {
      toaster.create({
        title: err instanceof Error ? err.message : "Error",
        type: "error",
        duration: 5000,
      });
    } finally {
      setIsSending(false);
    }
  }, [feedbackText, sendFeedback, isSending]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === "Enter") {
        void handleSend();
      }
    },
    [handleSend],
  );

  const execConfirm = useCallback(async () => {
    const action = confirmAction;
    setConfirmAction(null);
    if (!action || isSending) return;
    setIsSending(true);
    try {
      if (action === "approve") {
        await approve();
        toaster.create({ title: "Approved", type: "success", duration: 4000 });
      } else if (action === "reject") {
        await reject();
        toaster.create({ title: "Rejected", type: "success", duration: 4000 });
      }
    } catch (err) {
      toaster.create({
        title: err instanceof Error ? err.message : "Error",
        type: "error",
        duration: 5000,
      });
    } finally {
      setIsSending(false);
    }
  }, [confirmAction, approve, reject, isSending]);

  if (loading) {
    return (
      <Flex
        align="center"
        bg="bg"
        color="fg"
        flexDirection="column"
        h="100vh"
        justify="center"
        p={5}
      >
        <Box
          bg="bg.subtle"
          borderRadius="xl"
          borderWidth="1px"
          maxW="440px"
          p={12}
          textAlign="center"
        >
          <Spinner colorPalette="brand" mb={4} size="lg" />
          <Heading size="md">Connecting to session</Heading>
          <Text color="fg.muted" fontSize="sm" mt={2}>
            Looking up the HITL review session for this task...
          </Text>
        </Box>
      </Flex>
    );
  }

  if (!session) {
    return <NoSession />;
  }

  const isTerminal = isTerminalStatus(session);
  const canAct = session.status === "pending_review" && !session.task_completed;
  const badge = STATUS_BADGE[session.status] ?? STATUS_BADGE["pending_review"];

  return (
    <Box
      bg="bg"
      color="fg"
      display="flex"
      flexDirection="column"
      h="100vh"
      maxW="860px"
      mx="auto"
    >
      <Box borderBottomWidth="1px" px={5} py={4}>
        <Heading size="sm">HITL Review</Heading>
        <HStack flexWrap="wrap" fontSize="sm" gap={3} mt={1} color="fg.muted">
          <Text as="span">
            <Text as="b">Task:</Text> {session.task_id}
          </Text>
          <Text as="span">
            <Text as="b">DAG:</Text> {session.dag_id}
          </Text>
          <Text as="span">
            <Text as="b">Iteration:</Text> {session.iteration}/{session.max_iterations}
          </Text>
          <Badge colorPalette={badge.colorPalette} fontSize="2xs" px={2} py={0.5} borderRadius="full">
            {badge.label}
          </Badge>
        </HStack>
      </Box>

      {error && (
        <Box
          bg="red.subtle"
          color="red.fg"
          px={5}
          py={3}
          borderBottomWidth="1px"
          borderColor="red.emphasized"
          fontSize="sm"
          fontWeight="medium"
        >
          {error}
        </Box>
      )}

      <Box flex={1} overflowY="auto" p={5} display="flex" flexDirection="column" gap={3} ref={chatRef}>
        {session.conversation.map((entry, idx) => (
          <MessageBubble
            key={`${entry.role}-${entry.iteration}-${idx}`}
            entry={entry}
          />
        ))}
        {session.status === "pending_review" && !isTerminal && (
          <Text color="fg.muted" fontSize="sm" textAlign="center" py={4}>
            Waiting for your review
          </Text>
        )}
      </Box>

      {isTerminal && (
        <Box px={5} py={3}>
          <Box
            bg={
              session.status === "rejected" ||
              session.status === "max_iterations_exceeded" ||
              session.status === "timeout_exceeded"
                ? "red.subtle"
                : "green.subtle"
            }
            color={
              session.status === "rejected" ||
              session.status === "max_iterations_exceeded" ||
              session.status === "timeout_exceeded"
                ? "red.fg"
                : "green.fg"
            }
            borderRadius="xl"
            fontWeight="semibold"
            fontSize="md"
            py={5}
            textAlign="center"
          >
            {session.status === "approved"
              ? "Output Approved"
              : session.status === "rejected"
                ? "Output Rejected"
                : session.status === "max_iterations_exceeded"
                  ? "Max iterations exceeded"
                  : session.status === "timeout_exceeded"
                    ? "Timeout exceeded"
                    : "Task Completed — Session Ended"}
          </Box>
        </Box>
      )}

      {!isTerminal && (
        <Box borderTopWidth="1px" px={5} py={3}>
          {confirmAction === null ? (
            <VStack align="stretch" gap={3}>
              <HStack align="flex-end" gap={2}>
                <Textarea
                  ref={textareaRef}
                  flex={1}
                  minH="44px"
                  maxH="160px"
                  placeholder="Type feedback for the LLM... (Ctrl+Enter to send)"
                  resize="none"
                  rows={1}
                  value={feedbackText}
                  disabled={!canAct || isSending}
                  onChange={(e) => {
                    setFeedbackText(e.target.value);
                    autoResize();
                  }}
                  onKeyDown={handleKeyDown}
                />
                <Button
                  colorPalette="brand"
                  disabled={!canAct || isSending}
                  onClick={() => void handleSend()}
                >
                  Send
                </Button>
              </HStack>
              <HStack gap={2}>
                <Button
                  colorPalette="green"
                  disabled={!canAct || isSending}
                  onClick={() => setConfirmAction("approve")}
                >
                  Approve
                </Button>
                <Button
                  colorPalette="red"
                  disabled={!canAct || isSending}
                  onClick={() => setConfirmAction("reject")}
                >
                  Reject
                </Button>
              </HStack>
            </VStack>
          ) : (
            <HStack align="center" gap={3} py={3}>
              <Text fontSize="sm" fontWeight="medium">
                Are you sure you want to{" "}
                {confirmAction === "approve"
                  ? "approve this output"
                  : "reject this output"}
                ?
              </Text>
              <HStack gap={2}>
                <Button colorPalette="green" disabled={isSending} onClick={() => void execConfirm()}>
                  Yes
                </Button>
                <Button variant="outline" onClick={() => setConfirmAction(null)}>
                  Cancel
                </Button>
              </HStack>
            </HStack>
          )}
        </Box>
      )}
    </Box>
  );
};
