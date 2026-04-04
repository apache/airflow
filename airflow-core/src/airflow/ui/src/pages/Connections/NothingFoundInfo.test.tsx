/**
 * Tests for NothingFoundInfo component
 *
 * Ensures external documentation links use correct security attributes:
 * - target="_blank"
 * - rel="noopener noreferrer"
 */

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { NothingFoundInfo } from "./NothingFoundInfo";

describe("NothingFoundInfo", () => {
  it("should have correct external link attributes", () => {
    render(<NothingFoundInfo />);

    const link = screen.getByRole("link");

    expect(link).toHaveAttribute("target", "_blank");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
  });
});