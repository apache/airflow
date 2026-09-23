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

// Opening a raw TCP connection to a `host:port` the coordinator handed
// us. Both coordinator channels — the comm socket (`comm-channel.ts`)
// and the log socket (`log-channel.ts`) — connect the same way, so the
// connect dance lives here once instead of being duplicated in each.
//
// Coordinator-scoped on purpose: this is only how coordinator mode
// reaches Airflow, so keep it next to its only callers.

import { Socket } from "node:net";

/** Connect a `setNoDelay` TCP socket to `addr` (`host:port`). Resolves
 *  with the connected socket; rejects if the address is malformed or
 *  the connection attempt errors. The returned socket is raw — the
 *  caller owns all framing and lifecycle from here. */
export async function connectTcp(addr: string): Promise<Socket> {
  const [host, portStr] = splitHostPort(addr);
  return new Promise((resolve, reject) => {
    const sock = new Socket();
    sock.once("connect", () => {
      sock.setNoDelay(true);
      resolve(sock);
    });
    sock.once("error", reject);
    sock.connect(Number.parseInt(portStr, 10), host);
  });
}

/** Split a `host:port` address on its last colon (so IPv6 hosts like
 *  `::1` survive). Exported for unit testing. */
export function splitHostPort(addr: string): [string, string] {
  const idx = addr.lastIndexOf(":");
  if (idx < 0) throw new Error(`Address must be host:port, got ${addr}`);
  return [addr.slice(0, idx), addr.slice(idx + 1)];
}
