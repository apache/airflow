/*
  Global themes for all Chakra UI components in the app
*/

import { extendTheme } from '@chakra-ui/react';

const theme = extendTheme({
  config: {
    useSystemColorMode: true,
    initialColorMode: 'light',
  },
  fontSizes: {
    xs: '10px',
    sm: '12px',
    md: '14px',
    lg: '16px',
    xl: '18px',
    '2xl': '20px',
    '3xl': '24px',
    '4xl': '28px',
    '5xl': '36px',
    '6xl': '48px',
  },
  styles: {
    global: {
      body: {
        overflow: 'hidden',
      },
    },
  },
  components: {
    Button: {
      defaultProps: {
        colorScheme: 'teal',
      },
    },
    IconButton: {
      defaultProps: {
        colorScheme: 'teal',
      },
    },
    Switch: {
      defaultProps: {
        colorScheme: 'teal',
      },
    },
  },
});

export default theme;
