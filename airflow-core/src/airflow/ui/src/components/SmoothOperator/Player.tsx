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
import { useEffect, useRef, useState } from "react";

import { Box } from "@chakra-ui/react";

import { COLORS, COPY, prefersReducedMotion, TABLET_PORTRAIT } from "./stage";
import { EMBED_URL } from "./youtube";

/**
 * The video, loaded with autoplay once its task runs. YouTube asks that autoplay never start before
 * the player is visible (more than half on screen), so the iframe waits until the box is both faded
 * in and in view; on a phone that can mean until the viewer scrolls to it.
 */
export const Player = ({ isShowtime }: { readonly isShowtime: boolean }) => {
  const boxRef = useRef<HTMLDivElement>(null);
  const [hasBeenSeen, setHasBeenSeen] = useState(false);

  useEffect(() => {
    const box = boxRef.current;

    if (!isShowtime || hasBeenSeen || box === null) {
      return undefined;
    }
    box.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "nearest" });
    let ratio = 0;
    // The observer ignores opacity, so a box that is still fading in counts as hidden.
    const showIfVisible = () => {
      if (ratio >= 0.5 && globalThis.getComputedStyle(box).opacity === "1") {
        setHasBeenSeen(true);
      }
    };
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry !== undefined) {
          // isIntersecting is true for any sliver; YouTube's rule is more than half on screen.
          ratio = entry.intersectionRatio;
          showIfVisible();
        }
      },
      { threshold: 0.5 },
    );
    const onTransitionEnd = (event: TransitionEvent) => {
      if (event.target === box && event.propertyName === "opacity") {
        showIfVisible();
      }
    };

    observer.observe(box);
    box.addEventListener("transitionend", onTransitionEnd);

    return () => {
      observer.disconnect();
      box.removeEventListener("transitionend", onTransitionEnd);
    };
  }, [hasBeenSeen, isShowtime]);

  return (
    <Box
      // The 1984 video is 4:3; matching it avoids pillarbox bars.
      aspectRatio={4 / 3}
      bg={COLORS.ink}
      boxShadow="0 30px 80px rgba(0, 0, 0, 0.6)"
      // On a portrait tablet the title row sits above the video and takes about 300px.
      css={{ [TABLET_PORTRAIT]: { maxWidth: "min(100%, calc((78dvh - 330px) * 4 / 3)) !important" } }}
      data-testid="smooth-player"
      // Never taller than the space between the letterbox bars, so it can always be more than half
      // on screen (a landscape phone would otherwise never let it autoplay).
      maxWidth={{
        base: "min(100%, calc((100dvh - 160px) * 4 / 3))",
        md: "min(100%, calc((100dvh - 22vh - 64px) * 4 / 3))",
      }}
      // YouTube requires an embedded player to be at least 200x200 px. On a short landscape phone the
      // height cap above would otherwise shrink both dimensions below that, so both are floored.
      minHeight="200px"
      minWidth="200px"
      // Keeps its place in the layout, but stays out of sight until the SmoothOperator task runs.
      opacity={isShowtime ? 1 : 0}
      overflow="hidden"
      position="relative"
      ref={boxRef}
      transition="opacity 0.8s ease"
      width="100%"
    >
      {isShowtime && hasBeenSeen ? (
        <>
          {/* The frame is cross-origin, so allow-scripts with allow-same-origin only grants
              youtube-nocookie.com its own origin and cannot reach the Airflow page. The player needs
              both. allow-presentation lets it offer Cast and Presentation. */}
          {/* eslint-disable-next-line react/iframe-missing-sandbox */}
          <iframe
            allow="autoplay; encrypted-media; picture-in-picture; fullscreen"
            height="100%"
            // YouTube requires a Referer from embeds; this sends only Airflow's origin.
            referrerPolicy="strict-origin-when-cross-origin"
            sandbox="allow-scripts allow-same-origin allow-presentation allow-popups allow-popups-to-escape-sandbox"
            src={EMBED_URL}
            style={{ border: 0, display: "block", inset: 0, position: "absolute" }}
            title={COPY.video}
            width="100%"
          />
        </>
      ) : undefined}
    </Box>
  );
};
