import React from 'react';
import { QueryClient, QueryClientProvider } from 'react-query';

export const defaultHeaders = {
  'access-control-allow-origin': '*',
  'access-control-allow-credentials': 'true',
  'access-control-allow-headers': '*',
  'access-control-allow-methods': '*',
};

export const url: string = process.env.API_URL || '';

export const QueryWrapper: React.FC<{}> = ({ children }) => {
  const queryClient = new QueryClient();
  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
};
