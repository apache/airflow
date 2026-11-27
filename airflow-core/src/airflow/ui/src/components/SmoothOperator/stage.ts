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
import { keyframes } from "@emotion/react";

// A late-set supper club: red tungsten light on oxblood, cream type, no other colour.
// Fixed rather than theme tokens because the stage looks the same in light and dark mode.
export const COLORS = {
  cream: "#f1e4d0",
  ink: "#0b0607",
  key: "rgba(214, 38, 48, 0.55)",
  muted: "#a8927e",
  oxblood: "#2a0a0e",
  pool: "rgba(255, 170, 92, 0.32)",
  rose: "rgba(240, 96, 120, 0.3)",
  tungsten: "#ffb877",
};
// The scene's own words. Plain English on purpose: this is an Easter egg, not a product surface,
// and the film-credit voice does not translate.
export const COPY = {
  confirmBody:
    "Playing loads an embedded YouTube player that autoplays the video. YouTube may receive your IP address, browser or device information, and this page's address or referrer, including its host name. It may also store data on your device, such as cookies or local storage. YouTube's terms and privacy policy apply.",
  confirmTitle: "Play it smooth?",
  credit: "Sade, Smooth Operator (1984)",
  dag: "The Smooth Operator Dag",
  forget: "Close and ask before playing next time",
  label: "SMOOTH OPERATOR · SIDE A · 33⅓ RPM · ",
  open: "Play it smooth",
  pause: "Pause Dag",
  presents: "Apache Airflow presents",
  privacy: "Privacy",
  remember: "Remember this for next time",
  rememberNote:
    "Remembering keeps a single flag in this browser's local storage; Airflow sends nothing anywhere and you can clear it from the scene.",
  scratch: "Scratch the record",
  skip: "Tap or press any key to skip",
  states: { queued: "Queued", running: "Running", success: "Success" },
  terms: "YouTube Terms",
  title: ["Smooth", "Operator"],
  unpause: "Unpause Dag",
  video: "Sade – Smooth Operator (official music video)",
  watch: "Watch on YouTube",
};
export const prefersReducedMotion = () => globalThis.matchMedia("(prefers-reduced-motion: reduce)").matches;
export const DISPLAY_FONT = `"Iowan Old Style", "Palatino Linotype", Palatino, "Book Antiqua", Georgia, serif`;
// The song sits near 118 BPM, so four bars last about 8 s. The light breathes at that length; it is
// not synchronised to the audio, so it stays slow and shallow enough never to read as a missed beat.
export const BREATH = "8.12s";

// Also the lights coming up from black, and (as fadeOut) the curtain lifting.
export const fadeIn = keyframes`from { opacity: 0; } to { opacity: 1; }`;
export const fadeOut = keyframes`from { opacity: 1; } to { opacity: 0; }`;
export const TABLET_PORTRAIT = "@media (min-width: 48rem) and (orientation: portrait)";

/**
 * The scene runs as a tiny Dag. Each phase is one task running; "done" is a finished run.
 * - queued: black house, title card.
 * - dim: the red lights come up (dim_the_lights).
 * - needle: the platter spins up and the tonearm cues onto the record (drop_the_needle).
 * - show: the video is loaded with autoplay (the SmoothOperator task itself).
 */
export type Phase = "dim" | "done" | "needle" | "queued" | "show";
export const PHASES: Array<Phase> = ["queued", "dim", "needle", "show", "done"];
export const PHASE_MS: Record<Exclude<Phase, "done">, number> = {
  dim: 2400,
  needle: 2600,
  queued: 2200,
  show: 1200,
};
// The tonearm's cue has to finish inside drop_the_needle's phase.
export const CUE_MS = 2400;
export const isPast = (phase: Phase, other: Phase) => PHASES.indexOf(phase) > PHASES.indexOf(other);

// A running task's chip sends out a soft ring, so the eye finds the task that is working.
export const ring = keyframes`0% { box-shadow: 0 0 0 0 rgba(0, 199, 212, 0.55); } 100% { box-shadow: 0 0 0 10px rgba(0, 199, 212, 0); }`;
// Tonearm angles: resting off the record, over the lead-in groove, and near the label at the end
// of the side.
export const ARM_REST = -9;
export const ARM_LEAD_IN = 3;
const ARM_RUN_OUT = 20;

// The video's length, so the arm reaches the run-out about when the song ends. Ambient only: the page
// does not know the player's position.
export const SIDE_MS = 258_000;
// A cueing lever: lift, swing over the lead-in groove, lower. The shadow grows while the arm is up.
export const cue = keyframes`
  0% { transform: rotate(${ARM_REST}deg) scale(1); filter: drop-shadow(0 8px 10px rgba(0, 0, 0, 0.55)); }
  18% { transform: rotate(${ARM_REST}deg) scale(1.05); filter: drop-shadow(0 20px 18px rgba(0, 0, 0, 0.4)); }
  78% { transform: rotate(${ARM_LEAD_IN}deg) scale(1.05); filter: drop-shadow(0 20px 18px rgba(0, 0, 0, 0.4)); }
  100% { transform: rotate(${ARM_LEAD_IN}deg) scale(1); filter: drop-shadow(0 8px 10px rgba(0, 0, 0, 0.55)); }
`;
export const track = keyframes`from { transform: rotate(${ARM_LEAD_IN}deg); } to { transform: rotate(${ARM_RUN_OUT}deg); }`;

// The light moves on its own. Cycle lengths are coprime, so the room rarely looks the same twice.
export const driftKey = keyframes`
  0%, 100% { transform: translate(-6vw, -4vh) scale(1); }
  23% { transform: translate(7vw, -1vh) scale(1.12); }
  51% { transform: translate(15vw, 5vh) scale(0.94); }
  77% { transform: translate(-1vw, 7vh) scale(1.06); }
`;
export const driftPool = keyframes`
  0%, 100% { transform: translate(4vw, 3vh) scale(1.05); }
  31% { transform: translate(-9vw, -2vh) scale(0.92); }
  64% { transform: translate(-3vw, -7vh) scale(1.15); }
`;
export const sweep = keyframes`
  0%, 100% { transform: rotate(-10deg); }
  38% { transform: rotate(6deg); }
  71% { transform: rotate(-3deg); }
`;
export const warm = keyframes`0%, 100% { opacity: 0; } 50% { opacity: 1; }`;
export const swell = keyframes`0%, 100% { opacity: 1; } 50% { opacity: 0.92; }`;
export const step = keyframes`0%, 100% { transform: translateY(0); } 50% { transform: translateY(-1.5%); }`;
// Someone in a hat walks past the light once per cycle, then the room is quiet again.
export const passBy = keyframes`
  0%, 74% { opacity: 0; transform: translateX(-30vw); }
  76% { opacity: 1; }
  98% { opacity: 1; }
  100% { opacity: 0; transform: translateX(125vw); }
`;
