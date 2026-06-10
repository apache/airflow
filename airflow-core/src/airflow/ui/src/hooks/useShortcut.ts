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
import { type DependencyList, useEffect, useId } from "react";
import { type HotkeyCallback, type Options, useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";

import { type ShortcutDefinition, useShortcutRegistry } from "src/context/keyboardShortcuts";

type UseShortcutParams = {
  readonly callback: HotkeyCallback;
  readonly dependencies?: DependencyList;
  readonly options?: Options;
} & ShortcutDefinition;

/**
 * Thin wrapper around `react-hotkeys-hook`'s `useHotkeys` that also publishes the
 * shortcut to the keyboard-shortcut registry so it shows up in the help modal.
 *
 * The static `category`, `descriptionKey` and `keys` come from a `SHORTCUTS`
 * definition (spread in at the call site); `callback`, `options` and
 * `dependencies` stay at the call site since they depend on component state. The
 * shortcut is registered only while it is enabled, matching when the hotkey is
 * actually bound.
 */
export const useShortcut = ({
  callback,
  category,
  dependencies,
  descriptionKey,
  keys,
  options,
}: UseShortcutParams) => {
  const id = useId();
  const { t: translate } = useTranslation("common");
  const { register, unregister } = useShortcutRegistry();

  const ref = useHotkeys(keys, callback, options, dependencies);

  const description = translate(descriptionKey);

  const keyList: Array<string> = typeof keys === "string" ? [keys] : [...keys];
  // A `false` literal means the hotkey is not bound; a function trigger is evaluated
  // per event but the binding still exists, so we treat it as enabled here.
  const isEnabled = options?.enabled !== false;
  const keyListId = keyList.join(",");

  useEffect(() => {
    if (!isEnabled) {
      return undefined;
    }

    register({ category, description, id, keys: keyList });

    return () => unregister(id);
    // `keyListId` stands in for `keyList`'s identity to avoid re-running on each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [category, description, id, isEnabled, keyListId, register, unregister]);

  return ref;
};
