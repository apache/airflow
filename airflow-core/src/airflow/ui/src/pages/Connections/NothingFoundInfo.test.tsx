import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Wrapper } from "src/utils/Wrapper";
import { NothingFoundInfo } from "./NothingFoundInfo";

describe("NothingFoundInfo", () => {
  it("should have correct external link attributes", () => {
    render(<NothingFoundInfo />, { wrapper: Wrapper });

    const link = screen.getByRole("link");

    expect(link).toHaveAttribute("target", "_blank");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
  });
});