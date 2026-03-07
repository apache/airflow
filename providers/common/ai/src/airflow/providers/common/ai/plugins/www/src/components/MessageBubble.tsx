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

import type { FC } from "react";

import { Markdown } from "src/components/Markdown";
import type { ConversationEntry } from "src/types/feedback";

import styles from "./MessageBubble.module.css";

interface MessageBubbleProps {
  entry: ConversationEntry;
}

export const MessageBubble: FC<MessageBubbleProps> = ({ entry }) => {
  const isHuman = entry.role === "human";
  const label = isHuman ? "You" : "AI Assistant";

  return (
    <div className={`${styles.msg} ${isHuman ? styles.human : styles.assistant}`}>
      <div className={styles.label}>{label}</div>
      <Markdown content={entry.content} />
      <div className={styles.iterTag}>Iteration {entry.iteration}</div>
    </div>
  );
};
