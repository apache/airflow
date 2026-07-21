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
import { startTransition, useEffect, useRef, useState, type RefObject } from "react";

const NEAR_VIEWPORT_MARGIN = "600px 0px";

const listeners = new Map<Element, () => void>();
let sharedObserver: IntersectionObserver | undefined;

const releaseObserverWhenIdle = () => {
  if (listeners.size === 0) {
    sharedObserver?.disconnect();
    sharedObserver = undefined;
  }
};

const getSharedObserver = (observerConstructor: typeof IntersectionObserver) => {
  sharedObserver ??= new observerConstructor(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }

        const listener = listeners.get(entry.target);

        if (listener !== undefined) {
          listeners.delete(entry.target);
          sharedObserver?.unobserve(entry.target);
          listener();
        }
      });
      releaseObserverWhenIdle();
    },
    { rootMargin: NEAR_VIEWPORT_MARGIN, scrollMargin: NEAR_VIEWPORT_MARGIN },
  );

  return sharedObserver;
};

const observeNearViewport = (element: Element, listener: () => void) => {
  const observerConstructor = (globalThis as { IntersectionObserver?: typeof IntersectionObserver })
    .IntersectionObserver;

  if (observerConstructor === undefined) {
    listener();

    return undefined;
  }

  listeners.set(element, listener);
  getSharedObserver(observerConstructor).observe(element);

  return () => {
    listeners.delete(element);
    sharedObserver?.unobserve(element);
    releaseObserverWhenIdle();
  };
};

export const useNearViewport = <TElement extends Element>(): {
  readonly isNearViewport: boolean;
  readonly ref: RefObject<TElement | null>;
} => {
  const ref = useRef<TElement>(null);
  const [isNearViewport, setIsNearViewport] = useState(false);

  useEffect(() => {
    const element = ref.current;

    if (isNearViewport || element === null) {
      return undefined;
    }

    return observeNearViewport(element, () => {
      startTransition(() => setIsNearViewport(true));
    });
  }, [isNearViewport]);

  return { isNearViewport, ref };
};
