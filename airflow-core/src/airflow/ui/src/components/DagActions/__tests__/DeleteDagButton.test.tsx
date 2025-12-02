import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { ChakraProvider } from "@chakra-ui/react";
import { vi, describe, it, beforeEach, afterEach, expect } from "vitest";
import DeleteDagButton from "../DeleteDagButton";

vi.mock("src/queries/useDeleteDag", () => ({
  useDeleteDag: ({ onSuccessConfirm }: any) => ({
    isPending: false,
    mutate: () => onSuccessConfirm(),
  }),
}));

const renderWithProviders = (ui: React.ReactElement) =>
  render(
    <ChakraProvider>
      <MemoryRouter>{ui}</MemoryRouter>
    </ChakraProvider>
  );

describe("DeleteDagButton", () => {
  const navigateMock = vi.fn();

  beforeEach(async () => {
    vi.doMock("react-router-dom", async () => {
      const actual: any = await vi.importActual("react-router-dom");
      return {
        ...actual,
        useNavigate: () => navigateMock,
        useLocation: () => ({ search: "?tags=mytag" }),
      };
    });
  });

  afterEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it("preserves query params after deletion", async () => {
    renderWithProviders(<DeleteDagButton dagDisplayName="Test DAG" dagId="test_dag" />);

    const deleteButtons = screen.getAllByRole("button", { name: /delete/i });
    fireEvent.click(deleteButtons[0]);

    const confirmButtons = screen.getAllByRole("button", { name: /delete/i });
    fireEvent.click(confirmButtons[1]);

    expect(navigateMock).toHaveBeenCalledWith({
      pathname: "/dags",
      search: "?tags=mytag",
    });
  });
});
