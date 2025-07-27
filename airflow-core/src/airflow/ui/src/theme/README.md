# Airflow UI Theme System

This directory contains all theme-related files for the Airflow UI applications.

## Structure

### `index.ts`
- Main entry point for theme exports
- Re-exports the main theme system and utilities
- Provides named exports for specialized themes

### `main.ts`
- Primary Chakra UI theme system for the main Airflow UI
- Contains all color tokens, semantic tokens, and component customizations
- Includes custom OKLCH colors and Tailwind 4.0 color palette
- Defines task state colors and steel color palette

### `reactflow.ts`
- ReactFlow-specific theming utilities
- Maps the custom theme from main.ts to ReactFlow CSS classes.
- Exports a single `getReactFlowThemeStyle()` function to customize ReactFlow components



## Usage

### Main Airflow UI
```tsx
import { system } from "src/theme";

<ChakraProvider value={system}>
  // Your app
</ChakraProvider>
```

### ReactFlow Components
```tsx
import { getReactFlowThemeStyle } from "src/theme/reactflow";
import { useColorMode } from "src/context/colorMode";

const { colorMode } = useColorMode();

<ReactFlow style={getReactFlowThemeStyle(colorMode)}>
  // ReactFlow content
</ReactFlow>
```
