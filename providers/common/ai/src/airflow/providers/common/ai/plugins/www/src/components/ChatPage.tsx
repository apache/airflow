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

import styles from "./ChatPage.module.css";

interface ChatPageProps {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex: number;
}

type ConfirmAction = "approve" | "reject" | null;

const STATUS_BADGE: Record<string, { cls: string; label: string }> = {
  pending_review: { cls: styles.badgePending!, label: "Pending Review" },
  approved: { cls: styles.badgeApproved!, label: "Approved" },
  rejected: { cls: styles.badgeRejected!, label: "Rejected" },
  changes_requested: { cls: styles.badgeChanges!, label: "Regenerating..." },
};

export const ChatPage: FC<ChatPageProps> = ({ dagId, runId, taskId, mapIndex }) => {
  const { session, error, loading, taskActive, sendFeedback, approve, reject } =
    useSession(dagId, runId, taskId, mapIndex);

  const [feedbackText, setFeedbackText] = useState("");
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>(null);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);
  const chatRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (chatRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
  }, [session?.conversation]);

  useEffect(() => {
    if (toast) {
      const t = setTimeout(() => setToast(null), 3000);
      return () => clearTimeout(t);
    }
  }, [toast]);

  const autoResize = useCallback(() => {
    const ta = textareaRef.current;
    if (ta) {
      ta.style.height = "auto";
      ta.style.height = `${ta.scrollHeight}px`;
    }
  }, []);

  const handleSend = useCallback(async () => {
    const text = feedbackText.trim();
    if (!text) return;
    try {
      await sendFeedback(text);
      setFeedbackText("");
      setToast({ msg: "Feedback sent", ok: true });
    } catch (err) {
      setToast({ msg: err instanceof Error ? err.message : "Error", ok: false });
    }
  }, [feedbackText, sendFeedback]);

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
    try {
      if (action === "approve") {
        await approve();
        setToast({ msg: "Approved", ok: true });
      } else if (action === "reject") {
        await reject();
        setToast({ msg: "Rejected", ok: true });
      }
    } catch (err) {
      setToast({ msg: err instanceof Error ? err.message : "Error", ok: false });
    }
  }, [confirmAction, approve, reject]);

  if (loading) {
    return (
      <div className={styles.placeholder}>
        <div className={styles.placeholderCard}>
          <div className={styles.spinner} />
          <h2 className={styles.placeholderHeading}>Connecting to session</h2>
          <p className={styles.placeholderDesc}>
            Looking up the HITL review session for this task...
          </p>
        </div>
      </div>
    );
  }

  if (!session) {
    if (taskActive === false) {
      return <NoSession />;
    }

    return (
      <div className={styles.placeholder}>
        <div className={styles.placeholderCard}>
          <div className={styles.placeholderIcon}>&#x1F4AC;</div>
          <h2 className={styles.placeholderHeading}>Waiting for session to start</h2>
          <p className={styles.placeholderDesc}>
            The HITL review session has not been created yet. This usually means the
            task is still starting up. This page will automatically connect once the
            session is available.
          </p>
          <div className={styles.placeholderHint}>
            <span className={styles.dot} />
            <span className={styles.dot} />
            <span className={styles.dot} />
          </div>
          {error && <p className={styles.placeholderMuted}>{error}</p>}
        </div>
      </div>
    );
  }

  const isTerminal =
    session.status === "approved" ||
    session.status === "rejected" ||
    session.task_completed;
  const canAct = session.status === "pending_review" && !session.task_completed;
  const badge = STATUS_BADGE[session.status] ?? STATUS_BADGE["pending_review"]!;

  return (
    <div className={styles.app}>
      <header className={styles.header}>
        <h1 className={styles.title}>HITL Review</h1>
        <div className={styles.meta}>
          <span><b>Task:</b> {session.task_id}</span>
          <span><b>DAG:</b> {session.dag_id}</span>
          <span>
            <b>Iteration:</b> {session.iteration}/{session.max_iterations}
          </span>
          <span className={`${styles.badge} ${badge.cls}`}>{badge.label}</span>
        </div>
      </header>

      <div className={styles.chat} ref={chatRef}>
        {session.conversation.map((entry, idx) => (
          <MessageBubble key={idx} entry={entry} />
        ))}
        {session.status === "pending_review" && !isTerminal && (
          <div className={styles.waiting}>
            Waiting for your review{" "}
            <span className={styles.dot} />
            <span className={styles.dot} />
            <span className={styles.dot} />
          </div>
        )}
      </div>

      {isTerminal && (
        <div className={styles.terminalArea}>
          <div
            className={`${styles.terminalBanner} ${
              session.status === "rejected"
                ? styles.bannerRejected
                : styles.bannerApproved
            }`}
          >
            {session.status === "approved"
              ? "Output Approved"
              : session.status === "rejected"
                ? "Output Rejected"
                : "Task Completed — Session Ended"}
          </div>
        </div>
      )}

      {!isTerminal && (
        <footer className={styles.footer}>
          {confirmAction === null ? (
            <div>
              <div className={styles.inputArea}>
                <textarea
                  ref={textareaRef}
                  className={styles.textarea}
                  placeholder="Type feedback for the LLM..."
                  rows={1}
                  value={feedbackText}
                  disabled={!canAct}
                  onChange={(e) => {
                    setFeedbackText(e.target.value);
                    autoResize();
                  }}
                  onKeyDown={handleKeyDown}
                />
                <button
                  className={styles.btnSend}
                  disabled={!canAct}
                  onClick={() => void handleSend()}
                >
                  Send
                </button>
              </div>
              <div className={styles.actions}>
                <button
                  className={styles.btnApprove}
                  disabled={!canAct}
                  onClick={() => setConfirmAction("approve")}
                >
                  Approve
                </button>
                <button
                  className={styles.btnReject}
                  disabled={!canAct}
                  onClick={() => setConfirmAction("reject")}
                >
                  Reject
                </button>
              </div>
            </div>
          ) : (
            <div className={styles.confirmBar}>
              <span>
                Are you sure you want to{" "}
                {confirmAction === "approve"
                  ? "approve this output"
                  : "reject this output"}
                ?
              </span>
              <div className={styles.actions}>
                <button
                  className={styles.btnApprove}
                  onClick={() => void execConfirm()}
                >
                  Yes
                </button>
                <button
                  className={styles.btnCancel}
                  onClick={() => setConfirmAction(null)}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
        </footer>
      )}

      {toast && (
        <div className={`${styles.toast} ${toast.ok ? styles.toastOk : styles.toastErr}`}>
          {toast.msg}
        </div>
      )}
    </div>
  );
};
