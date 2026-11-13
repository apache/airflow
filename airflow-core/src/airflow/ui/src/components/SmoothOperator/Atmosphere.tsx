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
import { Box } from "@chakra-ui/react";

import {
  BREATH,
  COLORS,
  driftKey,
  driftPool,
  fadeIn,
  passBy,
  type Phase,
  PHASE_MS,
  step,
  sweep,
  swell,
  warm,
} from "./stage";

// A man in a hat, seen only as a shadow on the back wall.
const Silhouette = () => (
  <svg height="100%" viewBox="0 0 100 260">
    <ellipse cx="50" cy="40" fill="currentColor" rx="36" ry="6" />
    <rect fill="currentColor" height="30" rx="8" width="38" x="31" y="12" />
    <ellipse cx="50" cy="56" fill="currentColor" rx="15" ry="19" />
    <path d="M38 72 L62 72 L88 96 L80 260 L20 260 L12 96 Z" fill="currentColor" />
  </svg>
);

// Decorative layers only; everything here sits behind the content and ignores the pointer.
export const Atmosphere = ({ phase }: { readonly phase: Phase }) => (
  <Box aria-hidden inset={0} overflow="hidden" pointerEvents="none" position="absolute">
    {/* Dark until dim_the_lights runs; then the room fades up. */}
    <Box
      animation={phase === "dim" ? `${fadeIn} ${PHASE_MS.dim}ms ease-in-out both` : undefined}
      inset={0}
      opacity={phase === "queued" ? 0 : 1}
      position="absolute"
    >
      <Box animation={`${swell} ${BREATH} ease-in-out infinite`} inset={0} position="absolute">
        <Box
          animation={`${driftKey} 37s ease-in-out infinite`}
          bg={`radial-gradient(ellipse 55% 70% at 26% 18%, ${COLORS.key}, transparent 70%)`}
          data-motion
          inset="-15%"
          position="absolute"
        />
        <Box
          animation={`${driftPool} 53s ease-in-out infinite`}
          data-motion
          height="75vh"
          position="absolute"
          right="2%"
          top="30%"
          width="75vh"
        >
          <Box
            bg={`radial-gradient(circle, ${COLORS.pool}, transparent 65%)`}
            inset={0}
            position="absolute"
          />
          {/* Cross-fading a rose pool over the amber one shifts the colour without repainting. */}
          <Box
            animation={`${warm} 61s ease-in-out infinite`}
            bg={`radial-gradient(circle, ${COLORS.rose}, transparent 65%)`}
            inset={0}
            opacity={0}
            position="absolute"
          />
        </Box>
      </Box>
      <Box
        animation={`${sweep} 29s ease-in-out infinite`}
        bg="conic-gradient(from 160deg at 50% 0%, transparent 0deg, rgba(255, 214, 170, 0.13) 14deg, rgba(255, 214, 170, 0.05) 26deg, transparent 40deg)"
        data-motion
        height="150vh"
        left="58%"
        marginLeft="-70vh"
        position="absolute"
        top="-20vh"
        transformOrigin="50% 0%"
        width="140vh"
      />
    </Box>
    <Box
      // The first walk starts about 8 s in, once the video is up: the pass begins at 74% of the
      // 47 s cycle. The silhouette walks under reduced motion too; one slow crossing a minute is
      // a character, not a hazard, unlike the lights that never stop.
      animation={`${passBy} 47s linear -27s infinite`}
      color="rgba(0, 0, 0, 0.62)"
      filter="blur(3px)"
      height="46vh"
      left={0}
      opacity={0}
      position="absolute"
      top="16vh"
    >
      <Box animation={`${step} 0.9s ease-in-out infinite`} height="100%">
        <Silhouette />
      </Box>
    </Box>
    <Box
      bg="radial-gradient(ellipse at 50% 45%, transparent 40%, rgba(0, 0, 0, 0.8) 100%)"
      inset={0}
      position="absolute"
    />
  </Box>
);
