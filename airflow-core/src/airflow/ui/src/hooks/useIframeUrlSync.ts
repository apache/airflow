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
import { type RefObject, useEffect, useRef } from "react";

import { useLocation, useNavigate, useParams } from "react-router-dom";

// Resolve a URL-derived candidate and hand back only a same-origin, root-relative path (else null).
// The inner path comes from the address bar, so a crafted value must never send the iframe
// off-origin: resolving with the URL parser normalises every escaping trick (protocol-relative
// `//host`, backslashes, scheme, encoded/whitespace variants) and the origin check rejects anything
// that lands elsewhere; the leading-slash guard then guarantees a plain root-relative path.
const sameOriginPath = (candidate: string): string | null => {
  try {
    const resolved = new URL(candidate, globalThis.location.origin);

    if (resolved.origin !== globalThis.location.origin) {
      return null;
    }

    const path = `${resolved.pathname}${resolved.search}${resolved.hash}`;

    return path.startsWith("/") && !path.startsWith("//") ? path : null;
  } catch {
    return null;
  }
};

type UseIframeUrlSyncOptions = {
  readonly basePath: string;
  readonly enabled: boolean;
  readonly entrySrc: string;
  readonly iframeRef: RefObject<HTMLIFrameElement | null>;
  // Optional bound on where the framed page may go. When it navigates to a path this rejects, the
  // user is sent home instead of the address bar being updated — so the main app (and its own nav)
  // is never rendered inside the iframe.
  readonly isAllowedPath?: (pathname: string) => boolean;
};

/**
 * Two-way sync between a same-origin iframe and the address bar so framed pages are deep-linkable:
 * the URL carries the inner path under `basePath`, navigation inside the iframe updates it (via
 * replace, leaving Back/Forward to the iframe's own history), and a deep-linked URL loads that page.
 */
export const useIframeUrlSync = ({
  basePath,
  enabled,
  entrySrc,
  iframeRef,
  isAllowedPath,
}: UseIframeUrlSyncOptions): { initialSrc: string } => {
  const splat = useParams()["*"] ?? "";
  const { hash, search } = useLocation();
  const navigate = useNavigate();

  // Held in a ref so the load listener keeps a stable identity while calling the latest predicate.
  const isAllowedRef = useRef(isAllowedPath);

  isAllowedRef.current = isAllowedPath;

  // Last inner path reconciled with the address bar; keeps the two directions from looping.
  const syncedPath = useRef<string | null>(null);
  // Frozen once so address-bar updates never reset the iframe src.
  const initialSrc = useRef<string | null>(null);
  // Set once we send the iframe out of the allowed area, so blanking it (a further load) is a no-op.
  const redirecting = useRef(false);

  if (initialSrc.current === null && (splat !== "" || entrySrc !== "")) {
    const deepLink = splat === "" ? null : sameOriginPath(`/${splat}${search}${hash}`);

    initialSrc.current = deepLink ?? entrySrc;
    syncedPath.current = deepLink;
  }

  useEffect(() => {
    const element = iframeRef.current;

    if (!enabled || element === null) {
      return undefined;
    }

    const handleLoad = () => {
      const frameWindow = element.contentWindow;

      if (frameWindow === null) {
        return;
      }

      let href;
      let pathname;
      let frameSearch;
      let frameHash;

      try {
        ({ hash: frameHash, href, pathname, search: frameSearch } = frameWindow.location);
      } catch {
        return; // Cross-origin content is unreadable.
      }

      if (href === "about:blank") {
        return;
      }

      const allowed = isAllowedRef.current;

      if (allowed !== undefined && !allowed(pathname)) {
        if (!redirecting.current) {
          redirecting.current = true;
          element.src = "about:blank";
          void navigate("/");
        }

        return;
      }

      const key = `${pathname}${frameSearch}${frameHash}`;

      if (key === syncedPath.current) {
        return;
      }
      syncedPath.current = key;
      void navigate(
        { hash: frameHash, pathname: `${basePath}${pathname}`, search: frameSearch },
        { replace: true },
      );
    };

    element.addEventListener("load", handleLoad);
    handleLoad(); // A load can fire before this listener attaches; reconcile the current state now.

    return () => element.removeEventListener("load", handleLoad);
  }, [enabled, basePath, navigate, iframeRef]);

  useEffect(() => {
    if (!enabled || splat === "") {
      return;
    }

    const desired = sameOriginPath(`/${splat}${search}${hash}`);
    const frameWindow = iframeRef.current?.contentWindow;

    if (desired === null || desired === syncedPath.current || !frameWindow) {
      return;
    }
    syncedPath.current = desired;

    try {
      frameWindow.location.replace(desired);
    } catch {
      // Cross-origin content cannot be driven from here.
    }
  }, [enabled, splat, search, hash, iframeRef]);

  return { initialSrc: initialSrc.current ?? entrySrc };
};
