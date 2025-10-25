import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';

import { AssetSchedule } from './AssetSchedule';

const longName = 'VeryLongAssetNameWhichShouldBeTruncatedBecauseItIsTooLongToFitInList';

// Mock translation function
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (str: string) => str,
  }),
}));

// Mock hook for next run assets
vi.mock('openapi/queries', () => ({
  useAssetServiceNextRunAssets: () => ({
    data: {
      events: [
        {
          id: 'asset1',
          name: longName,
          uri: longName,
          lastUpdate: null,
        },
      ],
    },
    isLoading: false,
  }),
}));

describe('AssetSchedule', () => {
  test('truncates long asset names using isTruncated', () => {
    render(
      <MemoryRouter>
        <AssetSchedule dagId="testDag" timetableSummary="test summary" />
      </MemoryRouter>,
    );
    const linkElement = screen.getByText(longName);
    // When isTruncated is used, Chakra sets a title attribute equal to the full text
    expect(linkElement).toHaveAttribute('title', longName);
  });
});
