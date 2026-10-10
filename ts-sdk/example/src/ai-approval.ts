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

// An agent that asks a person before it acts, without a worker waiting for the answer.
//
// The Vercel AI SDK never waits: with `toolApproval`, `generateText` returns a
// `tool-approval-request` instead of running the tool, and the app has to keep the message history,
// collect the decision, append a `tool-approval-response`, and call `generateText` again. Airflow
// owns the waiting, so the loop becomes three tasks:
//
//     agent_step  ->  review (hitl)  ->  agent_continue
//
// `review` parks in `awaiting_input` and frees its worker. The model runs once in `agent_step` and
// once in `agent_continue`; nothing re-runs it while the reviewer decides.
//
// Pack it into a bundle the Node coordinator parses:
//
//     airflow-ts-pack src/ai-approval.ts --outfile dist/ai-approval.min.mjs

import { Bundle, Dag } from "apache-airflow-ts-sdk";
import { hitl } from "apache-airflow-ts-sdk/hitl";
import type { HITLResult } from "apache-airflow-ts-sdk/hitl";

/** The queue the deployment's `[sdk] queue_to_coordinator` routes to the Node coordinator. */
const QUEUE = "typescript";

/** What the stand-in model drafts, and what the reviewer's form starts from. */
const DRAFTED_AMOUNT = 120;

interface ToolCall {
  type: "tool-call";
  toolCallId: string;
  toolName: string;
  input: { orderId: string; amount: number };
}

interface ApprovalRequest {
  type: "tool-approval-request";
  approvalId: string;
  toolCall: ToolCall;
}

interface ApprovalResponse {
  type: "tool-approval-response";
  approvalId: string;
  approved: boolean;
  reason?: string;
}

type ModelMessage =
  | { role: "user"; content: string }
  | { role: "assistant"; content: ApprovalRequest[] }
  | { role: "tool"; content: ApprovalResponse[] };

interface GenerateTextResult {
  text: string;
  content: ApprovalRequest[];
  response: { messages: ModelMessage[] };
}

/** Stands in for the Vercel AI SDK's `generateText`: same call shape, a scripted answer. */
async function generateText(opts: { messages: ModelMessage[] }): Promise<GenerateTextResult> {
  // The branch comes from the history alone: the two calls run in different processes.
  const response = opts.messages
    .flatMap((message) => (message.role === "tool" ? message.content : []))
    .at(-1);
  if (response === undefined) {
    const request: ApprovalRequest = {
      type: "tool-approval-request",
      approvalId: "approval-1",
      toolCall: {
        type: "tool-call",
        toolCallId: "call-1",
        toolName: "issue_refund",
        input: { orderId: "ord_1042", amount: DRAFTED_AMOUNT },
      },
    };
    return {
      text: "",
      content: [request],
      response: { messages: [{ role: "assistant", content: [request] }] },
    };
  }

  const { toolCall } = opts.messages
    .flatMap((message) => (message.role === "assistant" ? message.content : []))
    .find((request) => request.approvalId === response.approvalId)!;
  const text = response.approved
    ? `Refunded $${toolCall.input.amount} on order ${toolCall.input.orderId}.`
    : `The refund on order ${toolCall.input.orderId} was declined, so nothing was issued.`;
  return { text, content: [], response: { messages: [] } };
}

interface AgentStep {
  messages: ModelMessage[];
  pending: { id: string; toolName: string; input: ToolCall["input"] }[];
}

const dag = new Dag("ts_hitl_ai_approval", { queue: QUEUE, tags: ["typescript", "hitl", "ai"] });

// 1. The model drafts a refund, and the tool call waits for approval instead of running.
const agentStep = dag.task("agent_step", async (): Promise<AgentStep> => {
  const messages: ModelMessage[] = [{ role: "user", content: "Refund order ord_1042." }];
  const result = await generateText({ messages });
  return {
    messages: [...messages, ...result.response.messages],
    pending: result.content.map((request) => ({
      id: request.approvalId,
      toolName: request.toolCall.toolName,
      input: request.toolCall.input,
    })),
  };
})();

// 2. A person reviews the draft. The drafted input is in the body and `amount` starts at the
// drafted figure; `params` are fixed when the Dag is declared, so the form cannot read the draft.
const review = dag.task(
  "review",
  hitl({
    subject: ({ draft }: { draft: AgentStep }) => `Approve ${draft.pending[0]!.toolName}?`,
    body: ({ draft }: { draft: AgentStep }) =>
      [
        `The agent wants to call \`${draft.pending[0]!.toolName}\` with:`,
        "",
        "```json",
        JSON.stringify(draft.pending[0]!.input, null, 2),
        "```",
      ].join("\n"),
    options: ["Approve", "Reject"],
    params: {
      amount: {
        value: DRAFTED_AMOUNT,
        description: "Amount to refund",
        schema: { type: "number" },
      },
    },
  }),
)({ draft: agentStep });

// 3. Append the decision to the history and call the model a second time.
dag.task(
  "agent_continue",
  async ({ draft, decision }: { draft: AgentStep; decision: HITLResult }) => {
    const approved = decision.chosenOptions[0] === "Approve";
    const edited = decision.paramsInput["amount"];
    const amount = typeof edited === "number" ? edited : draft.pending[0]!.input.amount;
    const approvalId = draft.pending[0]!.id;

    // The reviewer's figure replaces the drafted one, so the tool runs with what they approved.
    const messages: ModelMessage[] = [
      ...draft.messages.map((message): ModelMessage => {
        if (message.role !== "assistant") return message;
        return {
          role: "assistant",
          content: message.content.map((request) =>
            request.approvalId === approvalId
              ? {
                  ...request,
                  toolCall: { ...request.toolCall, input: { ...request.toolCall.input, amount } },
                }
              : request,
          ),
        };
      }),
      {
        role: "tool",
        content: [
          {
            type: "tool-approval-response",
            approvalId,
            approved,
            reason: approved ? `Approved at $${amount}` : "Rejected by the reviewer",
          },
        ],
      },
    ];
    const result = await generateText({ messages });
    return { text: result.text, modelCalls: 1, approvedAmount: approved ? amount : null };
  },
)({ draft: agentStep, decision: review });

await new Bundle(dag).serve();
