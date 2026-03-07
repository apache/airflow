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

import { type FC, useEffect } from "react";

import styles from "./NoSession.module.css";

const AUTO_REFRESH_MS = 5000;

export const NoSession: FC = () => {
  useEffect(() => {
    const timer = setTimeout(() => location.reload(), AUTO_REFRESH_MS);
    return () => clearTimeout(timer);
  }, []);

  return (
    <div className={styles.container}>
      <div className={styles.card}>
        <div className={styles.icon}>&#x1F4AC;</div>
        <h2 className={styles.heading}>No Active HITL Review Session</h2>
        <p className={styles.description}>
          This task does not have an active HITL review session right now. The chat
          window appears when the task is running with{" "}
          <code>enable_hitl_review=True</code>.
        </p>
        <div className={styles.hint}>
          <span className={styles.pulse}>&#x25CF;</span>&nbsp; If the task is
          currently running, the session may still be initialising. This page
          will auto-refresh.
        </div>
      </div>
    </div>
  );
};
