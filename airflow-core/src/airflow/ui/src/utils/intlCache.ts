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
export const DEFAULT_LOCALE = "en";

// Intl constructors are costly and durations and counters render in every table row and chart tick
// callback, so instances are reused. A stored language Intl rejects (e.g. a plugin-contributed "pt_BR")
// must not blank out or crash every duration and counter in the UI,
// hence the fallback to DEFAULT_LOCALE rather than letting the RangeError escape.
export const createIntlCache = <T>() => {
  const cache = new Map<string, T>();

  return (variant: string, locale: string, construct: (forLocale: string) => T): T => {
    const key = `${locale}|${variant}`;
    const cached = cache.get(key);

    if (cached !== undefined) {
      return cached;
    }

    let formatter: T;

    try {
      formatter = construct(locale);
    } catch {
      formatter = construct(DEFAULT_LOCALE);
    }

    cache.set(key, formatter);

    return formatter;
  };
};
