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
import { useEffect, useId, useRef, useState } from "react";

import { Box, chakra } from "@chakra-ui/react";

import { AirflowPin } from "src/assets/AirflowPin";

import {
  ARM_LEAD_IN,
  ARM_REST,
  COLORS,
  COPY,
  cue,
  CUE_MS,
  prefersReducedMotion,
  SIDE_MS,
  TABLET_PORTRAIT,
  track,
} from "./stage";

// 33⅓ rpm is one turn every 1.8 s. The platter turns at speed for everyone: a record that is
// playing has to look like it is playing. Reduced motion drops the ambient movement around it.
const TURN_MS = 1800;
// The motor brings the platter up to speed in well under a second.
const SPIN_UP_MS = 700;
// When the opening is skipped, the arm still swings over, just faster.
const QUICK_CUE_MS = 700;

// As large as the stage allows: what is left between the letterbox bars (22vh) after the title,
// the gap and the stage padding, capped by the column width.
const SIZE = {
  base: "min(72vw, 300px)",
  md: "min(36vw, calc(78dvh - 13.5rem))",
  xl: "min(34vw, calc(78dvh - 16.5rem))",
};

type Props = {
  readonly isPaused: boolean;
  readonly isSpinning: boolean;
  // "cueing" plays the lift–swing–lower move once; "down" and "rest" are where it ends up.
  readonly needle: "cueing" | "down" | "rest";
};

export const Record = ({ isPaused, isSpinning, needle }: Props) => {
  // useId() can contain characters that break url(#…) references.
  const pathId = `so${useId().replaceAll(/[^\w-]/gu, "")}`;
  const discRef = useRef<HTMLDivElement>(null);
  const spinRef = useRef<Animation | undefined>(undefined);

  useEffect(() => {
    const disc = discRef.current;

    if (disc === null || typeof disc.animate !== "function") {
      return undefined;
    }
    const spin = disc.animate([{ transform: "rotate(0deg)" }, { transform: "rotate(360deg)" }], {
      duration: TURN_MS,
      iterations: Infinity,
    });

    // cancel() rejects `finished`; nothing awaits it.
    spin.finished.catch(() => undefined);
    spin.pause();
    spinRef.current = spin;

    return () => {
      spin.cancel();
      spinRef.current = undefined;
    };
  }, []);

  // Once the motor starts it does not stop; ramp the rate up so the platter has inertia.
  useEffect(() => {
    const spin = spinRef.current;

    if (!isSpinning || spin === undefined) {
      return undefined;
    }
    const start = performance.now();
    let frame = 0;
    const step = (now: number) => {
      const progress = Math.min(1, (now - start) / SPIN_UP_MS);

      spin.updatePlaybackRate(1 - (1 - progress) ** 3);
      if (progress < 1) {
        frame = requestAnimationFrame(step);
      }
    };

    spin.updatePlaybackRate(0);
    frame = requestAnimationFrame(step);

    return () => cancelAnimationFrame(frame);
  }, [isSpinning]);

  // The motor stops with the Dag. CSS animations pause via the stage's data-paused; the Web
  // Animations spin needs telling directly.
  useEffect(() => {
    const spin = spinRef.current;

    if (spin === undefined || !isSpinning) {
      return undefined;
    }
    if (isPaused) {
      spin.pause();
    } else {
      spin.play();
    }

    return undefined;
  }, [isPaused, isSpinning]);

  // Whether the arm got here through the full cue. A skipped opening still swings it over, quickly,
  // and from then on it tracks slowly inward the way a side plays.
  const [hasCued, setHasCued] = useState(false);

  if (needle === "cueing" && !hasCued) {
    setHasCued(true);
  }
  let armAnimation: string | undefined;

  if (needle === "cueing") {
    armAnimation = `${cue} ${CUE_MS}ms ease-in-out both`;
  } else if (needle === "down") {
    // Same duration as the running cue, so a skip mid-swing lets it finish instead of snapping.
    const cueMs = hasCued ? CUE_MS : QUICK_CUE_MS;

    // The slow creep across the side is ambient movement; reduced motion keeps the arm on the lead-in.
    const trackAnimation = prefersReducedMotion() ? "" : `, ${track} ${SIDE_MS}ms linear ${cueMs}ms forwards`;

    armAnimation = `${cue} ${cueMs}ms ease-in-out both${trackAnimation}`;
  }

  // A DJ scratch: drag the platter back, let it catch up. The spin owns the disc's transform, so
  // the scratch is composited on top of it rather than replacing it, and the spin carries on from
  // wherever the platter lands. A deliberate click is not ambient motion, so reduced motion keeps it.
  const scratch = () => {
    const disc = discRef.current;

    if (disc === null || typeof disc.animate !== "function") {
      return;
    }
    disc.animate(
      [
        { transform: "rotate(0deg)" },
        { transform: "rotate(-70deg)" },
        { transform: "rotate(18deg)" },
        { transform: "rotate(0deg)" },
      ],
      { composite: "add", duration: 650, easing: "ease-in-out" },
    );
  };

  return (
    <chakra.button
      _focusVisible={{ outline: `2px solid ${COLORS.tungsten}`, outlineOffset: "2px" }}
      aria-label={COPY.scratch}
      borderRadius="full"
      // Beside the title on a portrait tablet, leaving the height to the video.
      css={{ [TABLET_PORTRAIT]: { height: "240px !important", width: "240px !important" } }}
      cursor="pointer"
      flexShrink={0}
      height={SIZE}
      onClick={scratch}
      position="relative"
      title={COPY.scratch}
      width={SIZE}
    >
      <Box inset={0} position="absolute">
        <Box
          // Texture only: nothing painted here is light, because light does not turn with the platter.
          // Fine grooves, plus a few denser bands between tracks so the eye can follow the rotation.
          bg="radial-gradient(circle at center, transparent 0 31%, rgba(0, 0, 0, 0.7) 31% 32.2%, transparent 32.2% 46%, rgba(0, 0, 0, 0.7) 46% 47%, transparent 47% 61%, rgba(0, 0, 0, 0.7) 61% 62.2%, transparent 62.2% 100%), repeating-radial-gradient(circle at center, #0b0b0c 0 1.5px, #19181a 1.5px 2.5px)"
          borderRadius="full"
          boxShadow="0 24px 60px rgba(0, 0, 0, 0.7), inset 0 0 0 2px #000"
          inset={0}
          position="absolute"
          ref={discRef}
        >
          <svg aria-hidden style={{ inset: "28%", position: "absolute" }} viewBox="0 0 100 100">
            <circle cx="50" cy="50" fill={COLORS.cream} r="50" />
            <circle
              cx="50"
              cy="50"
              fill="none"
              r="33"
              stroke={COLORS.oxblood}
              strokeOpacity="0.25"
              strokeWidth="0.6"
            />
            <path d="M 50,50 m -40,0 a 40,40 0 1,1 80,0 a 40,40 0 1,1 -80,0" fill="none" id={pathId} />
            {/* textLength stretches the label to exactly one lap of the path, so it never overlaps itself. */}
            <text fill={COLORS.oxblood} fontFamily="monospace" fontSize="8" textLength="250">
              <textPath href={`#${pathId}`}>{COPY.label}</textPath>
            </text>
          </svg>
          <Box height="14%" left="43%" position="absolute" top="43%" width="14%">
            <AirflowPin height="100%" width="100%" />
          </Box>
        </Box>
      </Box>
      {/* Fixed light. Concentric grooves throw the key light back as one soft wedge pointing at the
          centre, and the grooves slide underneath it; the rim catches a little of the same light. */}
      <Box
        bg="conic-gradient(from 214deg at 50% 50%, transparent 0deg, rgba(255, 232, 205, 0.08) 12deg, rgba(255, 232, 205, 0.42) 22deg, rgba(255, 232, 205, 0.08) 32deg, transparent 44deg), conic-gradient(from 34deg at 50% 50%, transparent 0deg, rgba(255, 232, 205, 0.14) 22deg, transparent 44deg), radial-gradient(circle at 32% 26%, rgba(255, 232, 205, 0.12), transparent 52%)"
        borderRadius="full"
        boxShadow="inset 0 0 0 1px rgba(255, 232, 205, 0.10), inset 10px 12px 18px -6px rgba(255, 232, 205, 0.18)"
        inset={0}
        pointerEvents="none"
        position="absolute"
      />
      {/* The tonearm rests beside the record until drop_the_needle cues it onto the lead-in groove. */}
      <Box
        animation={armAnimation}
        aria-hidden
        filter="drop-shadow(0 8px 10px rgba(0, 0, 0, 0.55))"
        height="74%"
        pointerEvents="none"
        position="absolute"
        right="-11%"
        top="-12%"
        transform={`rotate(${needle === "rest" ? ARM_REST : ARM_LEAD_IN}deg)`}
        transformOrigin="50% 11%"
        width="22%"
      >
        <svg height="100%" viewBox="0 0 60 200" width="100%">
          <defs>
            <linearGradient id={`${pathId}-steel`} x1="0" x2="1">
              <stop offset="0" stopColor="#6f6b72" />
              <stop offset="0.45" stopColor="#ece8ea" />
              <stop offset="1" stopColor="#6f6b72" />
            </linearGradient>
          </defs>
          <rect fill="#1b191c" height="12" rx="3" width="16" x="22" y="0" />
          <circle cx="30" cy="22" fill="#1b191c" r="16" stroke="#3b383d" strokeWidth="2" />
          <path
            d="M30 22 L30 158 Q30 168 24 174"
            fill="none"
            stroke={`url(#${pathId}-steel)`}
            strokeLinecap="round"
            strokeWidth="4"
          />
          <circle cx="30" cy="22" fill={`url(#${pathId}-steel)`} r="6.5" />
          <path d="M16 170 L30 176 L26 192 L12 186 Z" fill="#26232a" />
          <rect fill={COLORS.tungsten} height="4" opacity="0.8" rx="1" width="6" x="16" y="185" />
        </svg>
      </Box>
    </chakra.button>
  );
};
