import { renderHook } from "@testing-library/react";
import { describe, it, expect, beforeEach } from "vitest";

import { TimezoneProvider } from "./TimezoneProvider";
import { useTimezone } from "./useTimezone";

describe("TimezoneProvider falls back on invalid stored timezone", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("falls back to UTC when localStorage has an invalid value like 'PT'", () => {
    localStorage.setItem("timezone", JSON.stringify("PT"));

    const { result } = renderHook(() => useTimezone(), {
      wrapper: TimezoneProvider,
    });

    expect(result.current.selectedTimezone).toBe("UTC");
  });

  it("falls back to UTC when localStorage has an empty string", () => {
    localStorage.setItem("timezone", JSON.stringify(""));

    const { result } = renderHook(() => useTimezone(), {
      wrapper: TimezoneProvider,
    });

    expect(result.current.selectedTimezone).toBe("UTC");
  });

  it("keeps a valid stored timezone", () => {
    localStorage.setItem("timezone", JSON.stringify("America/New_York"));

    const { result } = renderHook(() => useTimezone(), {
      wrapper: TimezoneProvider,
    });

    expect(result.current.selectedTimezone).toBe("America/New_York");
  });
});