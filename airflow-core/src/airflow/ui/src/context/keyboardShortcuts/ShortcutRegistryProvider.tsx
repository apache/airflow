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
import { useState, type PropsWithChildren } from "react";

import { ShortcutRegistryContext, type ShortcutEntry } from "./Context";

/**
 * Holds the set of keyboard shortcuts that are currently mounted and enabled.
 *
 * Each `useShortcut` call registers itself here on mount and removes itself on
 * unmount, mirroring the lifecycle of the underlying `react-hotkeys-hook`
 * binding. The help modal renders from this registry, so it always reflects the
 * shortcuts actually available on the current page.
 */
export const ShortcutRegistryProvider = ({ children }: PropsWithChildren) => {
  const [shortcuts, setShortcuts] = useState<ReadonlyArray<ShortcutEntry>>([]);

  const register = (entry: ShortcutEntry) => {
    setShortcuts((prev) => [...prev.filter((item) => item.id !== entry.id), entry]);
  };

  const unregister = (id: string) => {
    setShortcuts((prev) => prev.filter((item) => item.id !== id));
  };

  const value = { register, shortcuts, unregister };

  return <ShortcutRegistryContext.Provider value={value}>{children}</ShortcutRegistryContext.Provider>;
};
