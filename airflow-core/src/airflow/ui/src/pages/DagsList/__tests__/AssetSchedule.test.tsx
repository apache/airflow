import { render, screen } from '@testing-library/react';
import { describe, test, expect, vi } from 'vitest';
import AssetSchedule from '../AssetSchedule';
import { Wrapper } from 'src/utils/Wrapper';

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
      <AssetSchedule dagId="testDag" timetableSummary="test summary" />, 
      { wrapper: Wrapper },
    );
    const linkElement = screen.getByText(longName);
    expect(linkElement).toHaveAttribute('title', longName);
  });
});
