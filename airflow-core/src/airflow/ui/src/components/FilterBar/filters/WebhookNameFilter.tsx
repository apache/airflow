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
import { useTranslation } from "react-i18next";

import { useWebhooksInfinite } from "src/queries/useWebhooksInfinite";

import type { FilterPluginProps } from "../types";
import { MultiSelectPill } from "./MultiSelectPill";

/**
 * Webhook multiselect over the webhooks the user may read. Attached to its config as an
 * ``EditorComponent`` so that the webhooks query, which reads each webhook's connection from the
 * secrets backends, only runs while the pill is mounted.
 */
export const WebhookNameFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation();
  const { data, error, fetchNextPage, isLoading } = useWebhooksInfinite({ limit: 50 });

  return (
    <MultiSelectPill
      filter={filter}
      isLoading={isLoading}
      noOptionsMessage={translate(
        error === null ? "table.noResultsFound" : "browse:webhooks.filterLoadError",
      )}
      onChange={onChange}
      onMenuScrollToBottom={() => {
        void fetchNextPage();
      }}
      onRemove={onRemove}
      options={(data?.pages.flatMap((page) => page.webhooks) ?? []).map(({ name }) => ({
        label: name,
        value: name,
      }))}
    />
  );
};
