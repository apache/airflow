/*
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

package org.apache.airflow.sdk

/**
 * A [Task] that can stop early when Airflow asks it to.
 *
 * Airflow asks a running task to stop when, for example, a user marks its task
 * instance as failed. The supervisor, which is the Airflow process that started
 * the JVM, then sends the JVM SIGTERM. If the JVM is still running after a grace
 * period, the supervisor sends SIGKILL.
 *
 * While a task of this type runs, the SDK catches the first SIGTERM or SIGINT
 * instead of letting the JVM exit. The SDK then makes [Context.isCancelled]
 * return true and calls [onKill]. A signal that arrives before the SDK starts
 * the task, and any later signal, goes to the JVM, which by default runs its
 * shutdown hooks and exits. The JVM handles every signal this way for a task that
 * does not implement this interface. If the SDK can catch neither signal, for
 * example because the JVM runs with `-Xrs`, the SDK fails the task before the
 * task starts.
 *
 * ```java
 * public class RunReport implements StoppableTask {
 *   private final ReportService service = new ReportService();
 *   private volatile String jobId;
 *
 *   @Override
 *   public void execute(Context context, Client client) throws Exception {
 *     jobId = service.submit();
 *     if (context.isCancelled()) {
 *       service.cancel(jobId);
 *       throw new IllegalStateException("Airflow asked the task to stop");
 *     }
 *     service.waitFor(jobId); // Throws if the job fails or is cancelled.
 *   }
 *
 *   @Override
 *   public void onKill(Context context) throws Exception {
 *     if (jobId != null) service.cancel(jobId);
 *   }
 * }
 * ```
 *
 * @see Airflow.OnKill
 */
interface StoppableTask : Task {
  /**
   * Stops what [execute] is doing. A task that only checks [Context.isCancelled]
   * in a loop can leave this method empty.
   *
   * The SDK calls this method at most once, on a daemon thread of its own, while
   * [execute] keeps running or has just returned. Anything the two methods share,
   * such as a `volatile` field, must therefore be safe to use from two threads.
   * This method must also tolerate work that has already finished.
   *
   * This method can run before [execute] has started the work that this method
   * should stop. To close that gap, have [execute] store what this method needs
   * and only then check [Context.isCancelled]. The SDK makes that check return
   * true before it calls this method, so the check sees the request, this method
   * sees what [execute] stored, or both.
   *
   * This method does not end [execute]. If [execute] is still running after a
   * grace period, the supervisor kills the JVM with SIGKILL and shutdown hooks do
   * not run. The SDK reports the result of [execute] only after this method returns.
   *
   * @param context Runtime context of the task, which is what [execute] received.
   * @throws Exception on failure. The SDK logs the exception and lets the JVM handle
   *    the signal, so the JVM exits without waiting for [execute].
   */
  @Throws(Exception::class)
  fun onKill(context: Context)
}
