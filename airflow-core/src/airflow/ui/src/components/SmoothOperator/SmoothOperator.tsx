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
import { type RefObject, useEffect, useRef, useState } from "react";

import { Box, Flex, Grid, Heading, Link, Text, VStack } from "@chakra-ui/react";
import { FiArrowUpRight } from "react-icons/fi";
import { LuDiscAlbum } from "react-icons/lu";
import { useLocalStorage } from "usehooks-ts";

import { Checkbox, Dialog, IconButton, Switch, Tooltip } from "src/system-components";

import { ConfirmationModal } from "src/components/ConfirmationModal";

import { Atmosphere } from "./Atmosphere";
import { MiniDag } from "./MiniDag";
import { Player } from "./Player";
import { Record } from "./Record";
import {
  COLORS,
  COPY,
  DISPLAY_FONT,
  fadeIn,
  fadeOut,
  isPast,
  type Phase,
  PHASE_MS,
  PHASES,
  TABLET_PORTRAIT,
} from "./stage";
import { PRIVACY_URL, TERMS_URL, WATCH_URL } from "./youtube";

const BAR_HEIGHT = { base: "56px", md: "11vh" };

type Props = {
  readonly operatorName?: string | null;
  readonly taskId: string;
};

// Steps through the scene's phases on timers. The run itself is the story, so it plays for
// everyone; reduced motion only tames the movement inside each phase. Pausing the Dag, as on a
// real one, stops the clock where it is and resumes from there.
const useShow = () => {
  const [phase, setPhase] = useState<Phase>("queued");
  const [isPaused, setIsPaused] = useState(false);
  // How much of the current phase had already played before a pause.
  const elapsedRef = useRef(0);

  useEffect(() => {
    if (phase === "done" || isPaused) {
      return undefined;
    }
    const startedAt = performance.now();
    let fired = false;
    const timer = setTimeout(
      () => {
        fired = true;
        elapsedRef.current = 0;
        setPhase((current) => PHASES[PHASES.indexOf(current) + 1] ?? "done");
      },
      Math.max(0, PHASE_MS[phase] - elapsedRef.current),
    );

    return () => {
      clearTimeout(timer);
      // Only a pause (or skip) interrupts a phase; a phase that ran to its end carries nothing over.
      if (!fired) {
        elapsedRef.current += performance.now() - startedAt;
      }
    };
  }, [isPaused, phase]);

  return {
    isPaused,
    phase,
    skip: () => setPhase("done"),
    togglePause: () => setIsPaused((paused) => !paused),
  };
};

const PolicyLinks = () => (
  <>
    {[
      { href: PRIVACY_URL, label: COPY.privacy },
      { href: TERMS_URL, label: COPY.terms },
    ].map(({ href, label }) => (
      <Link color={COLORS.muted} href={href} key={href} rel="noopener noreferrer" target="_blank">
        {label}
      </Link>
    ))}
  </>
);

const Stage = ({
  onForget,
  rootRef,
  taskId,
}: {
  // Set when the viewer chose to skip the confirmation next time; lets the scene undo that.
  readonly onForget?: () => void;
  readonly rootRef: RefObject<HTMLDivElement | null>;
  readonly taskId: string;
}) => {
  const { isPaused, phase, skip, togglePause } = useShow();
  const isDark = phase === "queued" || phase === "dim";

  return (
    <Box
      backdropFilter="blur(16px) sepia(1) saturate(2.2) hue-rotate(-35deg) brightness(0.32)"
      bg="rgba(42, 10, 14, 0.5)"
      color={COLORS.cream}
      css={{
        "& ::selection": { background: COLORS.tungsten, color: COLORS.ink },
        // A paused Dag runs nothing: every animation on stage holds its frame.
        "&[data-paused] *": { animationPlayState: "paused !important" },
        // Reduced motion keeps the fades (they carry the Dag's story) and stops the ambient movement
        // in the backdrop: the drifting lights and the sweep.
        "@media (prefers-reduced-motion: reduce)": {
          "& [data-motion]": { animation: "none !important", transition: "none !important" },
        },
      }}
      data-paused={isPaused ? "" : undefined}
      display="flex"
      flexDirection="column"
      height="100dvh"
      // Any click during the opening skips to the finished run, unless it is on a link: following
      // "Watch on YouTube" or the policy links must not also mount the video.
      onClick={
        phase === "done"
          ? undefined
          : (event) => {
              if (event.target instanceof Element && event.target.closest("a") !== null) {
                return;
              }
              skip();
            }
      }
      onKeyDown={(event) => {
        // Escape closes the dialog and Tab stays in its focus trap; other keys belong to the scene,
        // not to the task page's shortcuts underneath.
        if (event.key === "Escape" || event.key === "Tab") {
          return;
        }
        event.stopPropagation();
        // Enter or Space on a link or button activates it instead of skipping the opening.
        if (
          (event.key === "Enter" || event.key === " ") &&
          event.target instanceof Element &&
          event.target.closest("a, button") !== null
        ) {
          return;
        }
        if (phase === "done" || event.altKey || event.ctrlKey || event.metaKey) {
          return;
        }
        skip();
        // Skipping the opening must not also press whatever has focus.
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
        }
      }}
      // The dialog's first focus lands here rather than on the record under the curtain.
      outline="none"
      overflow="hidden"
      position="relative"
      ref={rootRef}
      tabIndex={-1}
      width="100vw"
    >
      <Atmosphere phase={phase} />
      <Flex
        alignItems="center"
        bg="#000"
        flexShrink={0}
        height={BAR_HEIGHT}
        // Centred like a marquee: the Dag is the plot.
        justifyContent="center"
        paddingX={{ base: 12, md: 20 }}
        position="relative"
        // Above the curtain, so the Dag is seen queued in the dark and dim_the_lights running.
        zIndex={2}
      >
        <MiniDag phase={phase} taskId={taskId} />
        {/* Airflow's own pause switch, doing to the show what it does to a Dag. The stage is always
            dark, so its colours are the dark-mode ones whatever theme the rest of the app is using:
            the theme resolves them at :root, so they are set here rather than inherited. */}
        <Tooltip content={isPaused ? COPY.unpause : COPY.pause}>
          <Switch
            checked={!isPaused}
            css={{
              // Airflow's dark-mode switch, measured: a white track when on, a slate one when off, a
              // grey ring on both, and a drop shadow that keeps the white thumb visible on white.
              "& [data-part=control]": {
                background: "oklch(0.28 0.035 251)",
                boxShadow: "inset 0 0 0 1px oklch(0.63 0.042 252)",
              },
              "& [data-part=control][data-state=checked]": { background: "white" },
              "& [data-part=thumb]": {
                background: "white",
                boxShadow: "0 2px 4px rgba(0, 0, 0, 0.64), inset 0 0 1px rgba(199, 207, 216, 0.3)",
              },
            }}
            data-testid="smooth-pause"
            inputProps={{ "aria-label": isPaused ? COPY.unpause : COPY.pause }}
            onCheckedChange={togglePause}
            onClick={(event) => event.stopPropagation()}
            position="absolute"
            right={{ base: 12, md: 16 }}
          />
        </Tooltip>
      </Flex>
      <Grid
        alignItems="center"
        columnGap={{ base: 6, xl: 16 }}
        // Two columns leave a portrait tablet with a postage-stamp video; stack there instead.
        css={{ [TABLET_PORTRAIT]: { gridTemplateColumns: "minmax(0, 1fr) !important" } }}
        flex={1}
        minHeight={0}
        // The desktop layout is sized to fit between the bars; only the stacked phone layout scrolls.
        overflowY={{ base: "auto", md: "hidden" }}
        paddingX={{ base: 4, md: "6vw" }}
        paddingY={{ base: 6, md: 4 }}
        position="relative"
        rowGap={8}
        templateColumns={{ base: "minmax(0, 1fr)", md: "minmax(0, 5fr) minmax(0, 7fr)" }}
      >
        <VStack
          alignItems="flex-start"
          css={{ [TABLET_PORTRAIT]: { alignItems: "center", flexDirection: "row" } }}
          gap={{ base: 6, md: 10 }}
          minWidth={0}
        >
          <Dialog.Title asChild>
            <Heading
              as="h2"
              fontFamily={DISPLAY_FONT}
              fontSize={{ base: "5xl", lg: "7xl", md: "6xl", xl: "8xl" }}
              fontWeight="normal"
              letterSpacing="-0.02em"
              lineHeight={0.92}
              textShadow="0 0 48px rgba(214, 38, 48, 0.45)"
            >
              {COPY.title[0]}{" "}
              <Box as="em" display="block" paddingLeft="0.6em">
                {COPY.title[1]}
              </Box>
            </Heading>
          </Dialog.Title>
          <Box
            css={{
              "@media (max-height: 500px)": { display: "none" },
              // Beside the title on a portrait tablet: room for the tonearm's base under the bar.
              [TABLET_PORTRAIT]: { paddingTop: "1.5rem" },
            }}
            // 0.6 × the heading's font size (5xl/6xl/7xl/8xl), so the record hangs under "Operator".
            paddingLeft={{ base: "1.8rem", lg: "2.7rem", md: "2.25rem", xl: "3.6rem" }}
          >
            <Record
              // The platter turns before the arm comes down, as on a real turntable.
              isPaused={isPaused}
              isSpinning={isPast(phase, "dim")}
              needle={phase === "needle" ? "cueing" : isPast(phase, "needle") ? "down" : "rest"}
            />
          </Box>
        </VStack>
        <Flex
          css={{ [TABLET_PORTRAIT]: { justifyContent: "center !important" } }}
          justifyContent={{ md: "flex-end" }}
          minWidth={0}
          width="100%"
        >
          {/* The video loads once the needle is down and autoplays where the browser allows it;
              otherwise YouTube shows its own play button. */}
          <Player isShowtime={isPast(phase, "needle")} />
        </Flex>
      </Grid>
      <Flex
        alignItems="center"
        bg="#000"
        columnGap={6}
        flexShrink={0}
        flexWrap="wrap"
        fontSize="sm"
        justifyContent={{ base: "center", md: "space-between" }}
        minHeight={BAR_HEIGHT}
        paddingX={{ base: 4, md: 8 }}
        paddingY={2}
        position="relative"
        rowGap={1}
        // Chrome, like the top bar: above the title card, so its links work from the first frame and
        // a click on them never lands on the curtain and skips the opening instead.
        zIndex={2}
      >
        <Text color={COLORS.muted} fontFamily={DISPLAY_FONT} fontStyle="italic">
          {COPY.credit}
        </Text>
        <Flex columnGap={5} flexWrap="wrap">
          <Link color={COLORS.tungsten} href={WATCH_URL} rel="noopener noreferrer" target="_blank">
            {COPY.watch}
            <FiArrowUpRight />
          </Link>
          <PolicyLinks />
          {onForget === undefined ? undefined : (
            <Link
              as="button"
              color={COLORS.muted}
              // Withdrawing consent must not also count as a click-to-skip, or it would load the video.
              onClick={(event) => {
                event.stopPropagation();
                onForget();
              }}
              type="button"
            >
              {COPY.forget}
            </Link>
          )}
        </Flex>
      </Flex>
      {/* Stays mounted so a skip can fade it rather than cut to the lit room. */}
      <Flex
        alignItems="center"
        animation={phase === "dim" ? `${fadeOut} ${PHASE_MS.dim}ms ease-in-out both` : undefined}
        aria-hidden={!isDark}
        bg="#000"
        cursor={isDark ? "pointer" : undefined}
        direction="column"
        gap={3}
        inset={0}
        justifyContent="center"
        opacity={isDark ? 1 : 0}
        pointerEvents={isDark ? undefined : "none"}
        position="absolute"
        transition="opacity 0.4s ease"
        zIndex={1}
      >
        <Text
          // The card leaves as soon as the lights start coming up.
          animation={phase === "queued" ? `${fadeIn} 0.6s ease-out both` : `${fadeOut} 0.5s ease-in both`}
          color={COLORS.cream}
          fontFamily={DISPLAY_FONT}
          fontSize={{ base: "lg", md: "2xl" }}
          fontStyle="italic"
          letterSpacing="0.04em"
        >
          {COPY.presents}
        </Text>
        {/* Fades out with the card instead of vanishing the moment the lights start. */}
        <Text
          animation={
            phase === "queued" ? `${fadeIn} 0.6s ease-out 0.8s both` : `${fadeOut} 0.5s ease-in both`
          }
          color={COLORS.muted}
          fontSize="xs"
        >
          {COPY.skip}
        </Text>
      </Flex>
    </Box>
  );
};

// Stays in this browser only; nothing about the choice leaves it.
const CONSENT_KEY = "smooth-operator-consent";

const SmoothOperatorScene = ({ taskId }: Pick<Props, "taskId">) => {
  const [isConfirming, setIsConfirming] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  // Only a Confirm is ever remembered: remembering a Cancel would lock the scene away with no way back.
  // Forgetting removes the key outright, so nothing about the choice is left in storage.
  const [hasConsented, setHasConsented, forgetConsent] = useLocalStorage(CONSENT_KEY, false, {
    // The hook JSON-parses whatever is stored, so a foreign or tampered value such as 1 or "yes"
    // would otherwise arrive as a truthy non-boolean; only the exact true this scene writes counts.
    deserializer: (raw) => raw === "true",
  });
  const [shouldRemember, setShouldRemember] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  const ask = () => {
    if (hasConsented) {
      setIsOpen(true);
    } else {
      setShouldRemember(false);
      setIsConfirming(true);
    }
  };
  const confirm = () => {
    if (shouldRemember) {
      setHasConsented(true);
    }
    setIsOpen(true);
  };

  return (
    <>
      <IconButton bg="bg" label={COPY.open} onClick={ask} size="sm" variant="outline">
        <LuDiscAlbum />
      </IconButton>
      {/* The scene loads a YouTube video, so it asks first, the way Airflow asks before pausing a Dag.
          Nothing is requested from Google until this is confirmed. */}
      <ConfirmationModal
        header={COPY.confirmTitle}
        onConfirm={confirm}
        onOpenChange={() => setIsConfirming(false)}
        open={isConfirming}
      >
        <Text>{COPY.confirmBody}</Text>
        <Flex columnGap={4} fontSize="sm" marginTop={3}>
          <PolicyLinks />
        </Flex>
        <Checkbox
          checked={shouldRemember}
          marginTop={4}
          onCheckedChange={(event) => setShouldRemember(Boolean(event.checked))}
        >
          {COPY.remember}
        </Checkbox>
        <Text color="fg.muted" fontSize="xs" marginTop={1}>
          {COPY.rememberNote}
        </Text>
      </ConfirmationModal>
      <Dialog.Root
        initialFocusEl={() => rootRef.current}
        lazyMount
        onOpenChange={() => setIsOpen(false)}
        open={isOpen}
        size="full"
        unmountOnExit
      >
        <Dialog.Content backdrop={false} bg="transparent" boxShadow="none" overflow="hidden">
          <Stage
            // Forgetting also closes the scene, so withdrawing before the needle drops sends nothing.
            onForget={
              hasConsented
                ? () => {
                    forgetConsent();
                    setIsOpen(false);
                  }
                : undefined
            }
            rootRef={rootRef}
            taskId={taskId}
          />
          <Dialog.CloseTrigger
            closeButtonProps={{ color: COLORS.cream, size: "md" }}
            position="absolute"
            right={{ base: 2, md: 6 }}
            top={{ base: "8px", md: "calc(5.5vh - 18px)" }}
            zIndex={3}
          />
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

// Returns early with no hooks of its own, so only a SmoothOperator's header reads localStorage or
// registers the storage listeners; every other task's header renders nothing here.
export const SmoothOperatorButton = ({ operatorName, taskId }: Props) =>
  operatorName === "SmoothOperator" ? <SmoothOperatorScene taskId={taskId} /> : undefined;
