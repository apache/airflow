import { vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DeleteDagButton from '../DeleteDagButton';

// Mock navigate to capture navigation calls
const navigateMock = vi.fn();

// Mock the delete hook to immediately call onSuccessConfirm
vi.mock('src/queries/useDeleteDag', () => ({
  useDeleteDag: ({ onSuccessConfirm }) => ({
    isPending: false,
    mutate: () => {
      onSuccessConfirm();
    },
  }),
}));

// Mock react-router-dom hooks to provide navigate and location
vi.mock('react-router-dom', async () => {
  const actual: any = await vi.importActual<typeof import('react-router-dom')>('react-router-dom');
  return {
    ...actual,
    useNavigate: () => navigateMock,
    useLocation: () => ({
      search: '?dag_id=my_dag',
    }),
  };
});

describe('DeleteDagButton', () => {
  it('preserves query params after deletion', () => {
    render(
      <MemoryRouter>
        <DeleteDagButton dagDisplayName="Test DAG" dagId="my_dag" />
      </MemoryRouter>,
    );

    // open delete dialog
    const openButtons = screen.getAllByRole('button', { name: /delete/i });
    fireEvent.click(openButtons[0]);

    // confirm deletion
    const confirmButtons = screen.getAllByRole('button', { name: /delete/i });
    fireEvent.click(confirmButtons[1]);

    expect(navigateMock).toHaveBeenCalledWith({
      pathname: '/dags',
      search: '?dag_id=my_dag',
    });
  });
});
