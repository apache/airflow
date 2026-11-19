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
import type { ReactNode } from "react";

import { Button, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { AssetAliasResponse, AssetWatcherResponse } from "openapi/requests/types.gen";

import { Popover, RouterLink } from "src/system-components";

import Time from "src/components/Time";
import { TriggerStatusBadge } from "src/components/TriggerStatusBadge";

import { formatNumber } from "src/utils";

type ListPopoverProps = {
  readonly colorPalette?: string;
  /** What the button counts, if not every item. */
  readonly count?: number;
  readonly items: Array<{ key: string; label: ReactNode }>;
  readonly noun: string;
};

export const ListPopover = ({ colorPalette, count, items, noun }: ListPopoverProps) => {
  const { i18n } = useTranslation();

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount unmountOnExit>
      <Popover.Trigger asChild disabled={items.length === 0}>
        <Button colorPalette={colorPalette} variant="outline">
          {formatNumber(count ?? items.length, i18n.language)} {noun}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          {items.map(({ key, label }) => (
            <Text key={key} py={2}>
              {label}
            </Text>
          ))}
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};

export const AliasesPopover = ({ aliases }: { readonly aliases: Array<AssetAliasResponse> }) => {
  const { t: translate } = useTranslation("assets");

  return (
    <ListPopover
      items={aliases.map((alias) => ({ key: String(alias.id), label: alias.name }))}
      noun={translate("alias", { count: aliases.length })}
    />
  );
};

export const WatchersPopover = ({ watchers }: { readonly watchers: Array<AssetWatcherResponse> }) => {
  const { t: translate } = useTranslation("assets");
  const failing = watchers.filter(({ status }) => status === "failing").length;
  const orphaned = watchers.filter(({ status }) => status === "orphaned").length;
  // The button says what is wrong without opening the list: failing first, then orphaned.
  const summary =
    failing > 0
      ? { colorPalette: "failed", count: failing, noun: translate("watcherFailing", { count: failing }) }
      : orphaned > 0
        ? {
            colorPalette: "warning",
            count: orphaned,
            noun: translate("watcherOrphaned", { count: orphaned }),
          }
        : { noun: translate("watcher", { count: watchers.length }) };

  return (
    <ListPopover
      {...summary}
      items={watchers.map((watcher) => ({
        // Watchers with the same trigger class and arguments share one trigger.
        key: `${watcher.trigger_id}-${watcher.name}`,
        label: (
          <HStack as="span">
            <RouterLink to={`/triggers/${watcher.trigger_id}`}>{watcher.name}</RouterLink>
            <TriggerStatusBadge status={watcher.status} />
            <Time datetime={watcher.created_date} />
          </HStack>
        ),
      }))}
    />
  );
};
