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
import { Link, Table } from "@chakra-ui/react";

import type { DagVersionResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

export const DagVersionDetails = ({ dagVersion }: { readonly dagVersion?: DagVersionResponse | null }) => {
  if (dagVersion === null || dagVersion === undefined) {
    return undefined;
  }

  return (
    <Table.Root striped>
      <Table.Body>
        <Table.Row>
          <Table.Cell>Version ID</Table.Cell>
          <Table.Cell>{dagVersion.id}</Table.Cell>
        </Table.Row>
        <Table.Row>
          <Table.Cell>Bundle Name</Table.Cell>
          <Table.Cell>{dagVersion.bundle_name}</Table.Cell>
        </Table.Row>
        {dagVersion.bundle_version === null ? undefined : (
          <Table.Row>
            <Table.Cell>Bundle Version</Table.Cell>
            <Table.Cell>{dagVersion.bundle_version}</Table.Cell>
          </Table.Row>
        )}
        {dagVersion.bundle_url === null ? undefined : (
          <Table.Row>
            <Table.Cell>Bundle Link</Table.Cell>
            <Table.Cell>
              <Link href={dagVersion.bundle_url}>{dagVersion.bundle_url}</Link>
            </Table.Cell>
          </Table.Row>
        )}
        <Table.Row>
          <Table.Cell>Created At</Table.Cell>
          <Table.Cell>
            <Time datetime={dagVersion.created_at} />
          </Table.Cell>
        </Table.Row>
      </Table.Body>
    </Table.Root>
  );
};
