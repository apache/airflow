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
import { Text, Skeleton } from "@chakra-ui/react";
import type { TFunction } from "i18next";

export const getInlineMessage = (isPendingDryRun: boolean, totalEntries: number, translate: TFunction) =>
  isPendingDryRun ? (
    <Skeleton height="20px" width="100px" />
  ) : totalEntries === 0 ? (
    <Text color="fg.error" fontSize="sm" fontWeight="medium">
      {translate("backfill.affectedNone")}
    </Text>
  ) : (
    <Text color="fg.success" fontSize="sm">
      {translate("backfill.affected", { count: totalEntries })}
    </Text>
  );
