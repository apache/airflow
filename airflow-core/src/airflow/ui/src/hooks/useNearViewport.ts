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
import { startTransition, useCallback, useEffect, useRef, useState, type RefObject } from "react";

const NEAR_VIEWPORT_MARGIN = "600px 0px";

type ObserverState = {
  readonly listeners: Map<Element, () => void>;
  readonly observer: IntersectionObserver;
};

const observerStates = new Map<Element | null, ObserverState>();

const getScrollRoot = (element: Element): Element | null => {
  let ancestor = element.parentElement;
  let overflowAncestor: Element | null = null;

  while (ancestor !== null) {
    const { overflowY } = globalThis.getComputedStyle(ancestor);

    if (/^(?:auto|overlay|scroll)$/u.test(overflowY)) {
      overflowAncestor ??= ancestor;

      if (ancestor.scrollHeight > ancestor.clientHeight) {
        return ancestor;
      }
    }

    ancestor = ancestor.parentElement;
  }

  return overflowAncestor;
};

const releaseObserverWhenIdle = (root: Element | null, state: ObserverState) => {
  if (state.listeners.size === 0) {
    state.observer.disconnect();

    if (observerStates.get(root) === state) {
      observerStates.delete(root);
    }
  }
};

const getSharedObserver = (observerConstructor: typeof IntersectionObserver, root: Element | null) => {
  const currentState = observerStates.get(root);

  if (currentState !== undefined) {
    return currentState;
  }

  const listeners = new Map<Element, () => void>();
  const observer = new observerConstructor(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }

        const listener = listeners.get(entry.target);

        if (listener !== undefined) {
          listeners.delete(entry.target);
          observer.unobserve(entry.target);
          listener();
        }
      });
      const observerState = observerStates.get(root);

      if (observerState !== undefined) {
        releaseObserverWhenIdle(root, observerState);
      }
    },
    { root, rootMargin: NEAR_VIEWPORT_MARGIN },
  );

  const state = { listeners, observer };

  observerStates.set(root, state);

  return state;
};

const observeNearViewport = (element: Element, listener: () => void) => {
  const observerConstructor = (globalThis as { IntersectionObserver?: typeof IntersectionObserver })
    .IntersectionObserver;

  // If observation is unavailable, we immediately call the listener.
  if (observerConstructor === undefined) {
    listener();

    return undefined;
  }

  const root = getScrollRoot(element);
  const state = getSharedObserver(observerConstructor, root);

  state.listeners.set(element, listener);
  state.observer.observe(element);

  return () => {
    state.listeners.delete(element);
    state.observer.unobserve(element);
    releaseObserverWhenIdle(root, state);
  };
};

export const useNearViewport = <TElement extends Element>(): {
  readonly isNearViewport: boolean;
  readonly ref: RefObject<TElement | null>;
  readonly showContent: () => void;
} => {
  const ref = useRef<TElement>(null);
  const [isNearViewport, setIsNearViewport] = useState(false);
  const showContent = useCallback(() => setIsNearViewport(true), []);

  useEffect(() => {
    const element = ref.current;

    if (isNearViewport || element === null) {
      return undefined;
    }

    return observeNearViewport(element, () => {
      startTransition(showContent);
    });
  }, [isNearViewport, showContent]);

  return { isNearViewport, ref, showContent };
};
