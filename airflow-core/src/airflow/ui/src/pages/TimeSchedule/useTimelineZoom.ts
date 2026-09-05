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
import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type Dispatch,
  type MouseEvent,
  type RefObject,
  type SetStateAction,
} from "react";

import { buildHourMarkers, buildTimeMarkers } from "./timelineUtils";
import type { TimeScale, ViewMode, ZoomAnchor } from "./types";

const TIME_SCALE_OPTIONS: Array<TimeScale> = [60, 30, 20, 10, 5, 1];
const FINE_ZOOM_STEPS: Array<TimeScale> = [60, 50, 40, 30, 20, 15, 10, 5, 1];
const MIN_TIME_LABEL_SPACING = 48;
const MIN_CHART_WIDTH = 1440;
const TIMELINE_HORIZONTAL_PADDING = 40;
const HOUR_MARKERS = buildHourMarkers();

const getNextScale = (current: TimeScale, direction: "in" | "out", steps: Array<TimeScale>) => {
  const currentIndex = steps.indexOf(current);

  if (currentIndex === -1) {
    return current;
  }

  return (
    (direction === "in"
      ? steps[Math.min(currentIndex + 1, steps.length - 1)]
      : steps[Math.max(currentIndex - 1, 0)]) ?? current
  );
};

type ZoomTimelineParams = {
  readonly chartBodyRef: RefObject<HTMLDivElement | null>;
  readonly clientX?: number;
  readonly clientY?: number;
  readonly lastPointerRef: RefObject<{ x: number; y: number } | null>;
  readonly nextScale: TimeScale;
  readonly pendingZoomAnchorRef: RefObject<ZoomAnchor | null>;
  readonly setTimeScale: Dispatch<SetStateAction<TimeScale>>;
  readonly timeScaleRef: RefObject<TimeScale>;
  readonly viewMode: ViewMode;
};

const zoomTimeline = ({
  chartBodyRef,
  clientX,
  clientY,
  lastPointerRef,
  nextScale,
  pendingZoomAnchorRef,
  setTimeScale,
  timeScaleRef,
  viewMode,
}: ZoomTimelineParams) => {
  const viewport = chartBodyRef.current;

  if (!viewport) {
    timeScaleRef.current = nextScale;
    setTimeScale(nextScale);

    return;
  }

  const axis = viewMode === "week" ? "vertical" : "horizontal";
  const viewportRect = viewport.getBoundingClientRect();
  const pointerCoordinate =
    (axis === "horizontal" ? clientX : clientY) ??
    (axis === "horizontal" ? lastPointerRef.current?.x : lastPointerRef.current?.y);
  const viewportStart = axis === "horizontal" ? viewportRect.left : viewportRect.top;
  const viewportSize = axis === "horizontal" ? viewportRect.width : viewportRect.height;
  const offset =
    pointerCoordinate === undefined
      ? (axis === "horizontal" ? viewport.clientWidth : viewport.clientHeight) / 2
      : Math.max(0, Math.min(pointerCoordinate - viewportStart, viewportSize));
  const scrollPosition = axis === "horizontal" ? viewport.scrollLeft : viewport.scrollTop;
  const contentSize = Math.max(1, axis === "horizontal" ? viewport.scrollWidth : viewport.scrollHeight);

  pendingZoomAnchorRef.current = {
    axis,
    offset,
    ratio: (scrollPosition + offset) / contentSize,
  };
  timeScaleRef.current = nextScale;
  setTimeScale(nextScale);
};

export const useTimelineZoom = (viewMode: ViewMode) => {
  const [timeScale, setTimeScale] = useState<TimeScale>(60);
  const [timeLabelStep, setTimeLabelStep] = useState(1);
  const chartBodyRef = useRef<HTMLDivElement | null>(null);
  const chartRootRef = useRef<HTMLDivElement | null>(null);
  const headerRowRef = useRef<HTMLDivElement | null>(null);
  const weekHeaderRef = useRef<HTMLDivElement | null>(null);
  const scrollRegionRef = useRef<HTMLDivElement | null>(null);
  const lastPointerRef = useRef<{ x: number; y: number } | null>(null);
  const pendingZoomAnchorRef = useRef<ZoomAnchor | null>(null);
  const timeScaleRef = useRef<TimeScale>(timeScale);
  const timeMarkers = buildTimeMarkers(timeScale);
  const chartWidth = Math.max(MIN_CHART_WIDTH, ((24 * 60) / timeScale) * 40);

  const zoomAtPoint = (nextScale: TimeScale, clientX?: number, clientY?: number) => {
    zoomTimeline({
      chartBodyRef,
      clientX,
      clientY,
      lastPointerRef,
      nextScale,
      pendingZoomAnchorRef,
      setTimeScale,
      timeScaleRef,
      viewMode,
    });
  };

  const zoomIn = () => {
    zoomAtPoint(getNextScale(timeScaleRef.current, "in", TIME_SCALE_OPTIONS));
  };

  const zoomOut = () => {
    zoomAtPoint(getNextScale(timeScaleRef.current, "out", TIME_SCALE_OPTIONS));
  };

  const handleChartMouseMove = (event: MouseEvent<HTMLDivElement>) => {
    lastPointerRef.current = { x: event.clientX, y: event.clientY };
  };

  const handleChartMouseLeave = () => {
    lastPointerRef.current = null;
  };

  useLayoutEffect(() => {
    const anchor = pendingZoomAnchorRef.current;
    const viewport = chartBodyRef.current;
    const header = viewMode === "week" ? weekHeaderRef.current : headerRowRef.current;

    if (!anchor || !viewport) {
      return;
    }

    if (anchor.axis === "vertical") {
      const maxScrollTop = Math.max(0, viewport.scrollHeight - viewport.clientHeight);

      viewport.scrollTop = Math.min(
        maxScrollTop,
        Math.max(0, anchor.ratio * viewport.scrollHeight - anchor.offset),
      );
    } else {
      const maxScrollLeft = Math.max(0, viewport.scrollWidth - viewport.clientWidth);

      viewport.scrollLeft = Math.min(
        maxScrollLeft,
        Math.max(0, anchor.ratio * viewport.scrollWidth - anchor.offset),
      );
      if (header) {
        header.scrollLeft = viewport.scrollLeft;
      }
    }
    pendingZoomAnchorRef.current = null;
  }, [timeScale, viewMode]);

  useEffect(() => {
    const header = viewMode === "week" ? weekHeaderRef.current : headerRowRef.current;
    const viewport = chartBodyRef.current;

    if (!header || !viewport) {
      return undefined;
    }

    const syncHeaderScroll = () => {
      header.scrollLeft = viewport.scrollLeft;
    };

    syncHeaderScroll();
    viewport.addEventListener("scroll", syncHeaderScroll);

    return () => viewport.removeEventListener("scroll", syncHeaderScroll);
  }, [viewMode]);

  useLayoutEffect(() => {
    setTimeLabelStep(1);
    const header = headerRowRef.current;

    if (!header || typeof ResizeObserver === "undefined") {
      return undefined;
    }

    const updateTimeLabelStep = () => {
      const timelineWidth =
        timeScale === 60
          ? Math.max(0, header.clientWidth - TIMELINE_HORIZONTAL_PADDING)
          : Math.max(0, chartWidth - TIMELINE_HORIZONTAL_PADDING);
      const markerSpacing = timelineWidth / Math.max(1, timeMarkers.length - 1);

      setTimeLabelStep(Math.max(1, Math.ceil(MIN_TIME_LABEL_SPACING / Math.max(1, markerSpacing))));
    };

    updateTimeLabelStep();
    const observer = new ResizeObserver(updateTimeLabelStep);

    observer.observe(header);

    return () => observer.disconnect();
  }, [chartWidth, timeMarkers.length, timeScale, viewMode]);

  useEffect(() => {
    const chartRoot = chartRootRef.current;
    const chartBody = chartBodyRef.current;

    if (!chartRoot || !chartBody) {
      return undefined;
    }

    const isZoomTarget = (target: EventTarget | null) =>
      target instanceof HTMLElement && chartRoot.contains(target);
    const isMacPlatform = /Mac|iPhone|iPad|iPod/iu.test(navigator.userAgent);

    const handleWheel = (event: WheelEvent) => {
      if (!isZoomTarget(event.target) || !(event.ctrlKey || event.metaKey)) {
        return;
      }

      const zoomDelta = event.deltaY === 0 ? event.deltaX : event.deltaY;
      const scrollRegion = viewMode === "day" ? scrollRegionRef.current : chartBody;
      const scrollTop = scrollRegion?.scrollTop;

      event.preventDefault();
      event.stopPropagation();

      if (zoomDelta !== 0) {
        const direction = isMacPlatform ? (zoomDelta > 0 ? "in" : "out") : zoomDelta < 0 ? "in" : "out";

        zoomTimeline({
          chartBodyRef,
          clientX: event.clientX,
          clientY: event.clientY,
          lastPointerRef,
          nextScale: getNextScale(timeScaleRef.current, direction, FINE_ZOOM_STEPS),
          pendingZoomAnchorRef,
          setTimeScale,
          timeScaleRef,
          viewMode,
        });
      }

      requestAnimationFrame(() => {
        if (viewMode === "day" && scrollRegion && scrollTop !== undefined) {
          scrollRegion.scrollTop = scrollTop;
        }
      });
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        !isZoomTarget(event.target) ||
        !(event.ctrlKey || event.metaKey) ||
        !["ArrowDown", "ArrowUp"].includes(event.key)
      ) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();
      zoomTimeline({
        chartBodyRef,
        clientX: lastPointerRef.current?.x,
        clientY: lastPointerRef.current?.y,
        lastPointerRef,
        nextScale: getNextScale(
          timeScaleRef.current,
          event.key === "ArrowUp" ? "in" : "out",
          FINE_ZOOM_STEPS,
        ),
        pendingZoomAnchorRef,
        setTimeScale,
        timeScaleRef,
        viewMode,
      });
    };

    globalThis.addEventListener("wheel", handleWheel, { capture: true, passive: false });
    globalThis.addEventListener("keydown", handleKeyDown, { capture: true });

    return () => {
      globalThis.removeEventListener("wheel", handleWheel, { capture: true });
      globalThis.removeEventListener("keydown", handleKeyDown, { capture: true });
    };
  }, [viewMode]);

  return {
    chartBodyRef,
    chartRootRef,
    chartWidth,
    headerRowRef,
    hourMarkers: HOUR_MARKERS,
    onChartMouseLeave: handleChartMouseLeave,
    onChartMouseMove: handleChartMouseMove,
    scrollRegionRef,
    timeLabelStep,
    timeMarkers,
    timeScale,
    weekHeaderRef,
    zoomIn,
    zoomInDisabled: timeScale === 1,
    zoomOut,
    zoomOutDisabled: timeScale === 60,
  };
};
