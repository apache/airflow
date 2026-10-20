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
import { render, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";

import { PartitionedDagRunService } from "openapi/requests/services.gen";

import { Wrapper } from "src/utils/Wrapper";

import { AssetProgressCell } from "./AssetProgressCell";

it("fetches separate details for pending runs sharing a partition key", async () => {
  const request = vi.spyOn(PartitionedDagRunService, "getPendingPartitionedDagRun").mockResolvedValue({
    assets: [],
    dag_id: "consumer",
    id: 1,
    partition_key: "same-key",
    total_received: 1,
    total_required: 1,
  });

  render(
    <>
      <AssetProgressCell
        dagId="consumer"
        partitionedDagRunId={1}
        partitionKey="same-key"
        totalReceived={1}
        totalRequired={1}
      />
      <AssetProgressCell
        dagId="consumer"
        partitionedDagRunId={2}
        partitionKey="same-key"
        totalReceived={1}
        totalRequired={1}
      />
    </>,
    { wrapper: Wrapper },
  );

  await waitFor(() => {
    expect(request).toHaveBeenCalledWith({
      dagId: "consumer",
      partitionedDagRunId: 1,
      partitionKey: "same-key",
    });
    expect(request).toHaveBeenCalledWith({
      dagId: "consumer",
      partitionedDagRunId: 2,
      partitionKey: "same-key",
    });
  });
});
