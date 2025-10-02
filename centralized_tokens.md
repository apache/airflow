# Centralized Semantic Tokens Proposal for Airflow UI

## Overview

This proposal outlines the implementation of a centralized semantic tokens system for the Airflow UI, inspired by the successful implementation in the Orbiter Client. The goal is to create a consistent, maintainable, and scalable theming system that supports both light and dark modes while providing semantic meaning to design tokens.

## Current State Analysis

The current Airflow UI theme system **already has semantic tokens** but with critical issues:

### ✅ What's Working
- Semantic tokens are defined using `generateSemanticTokens()` function
- Chakra's color mode system is leveraged for light/dark mode
- Base color palette is comprehensive (brand, gray, red, orange, amber, yellow, lime, green, emerald, teal, cyan, sky, blue, indigo, violet, purple, fuchsia, pink, rose, slate, zinc, neutral, stone)
- Generic state tokens work correctly (danger: red, info: blue, warning: amber, error: red)
- Chart and visualization tokens are well-defined
- ReactFlow tokens are properly implemented

### ❌ Critical Issues Found
- **BROKEN**: Task state tokens reference non-existent colors:
  ```typescript
  taskState: {
    success: generateSemanticTokens("success"),    // ❌ "success" doesn't exist
    failed: generateSemanticTokens("failed"),     // ❌ "failed" doesn't exist  
    running: generateSemanticTokens("running"),   // ❌ "running" doesn't exist
    queued: generateSemanticTokens("queued"),      // ❌ "queued" doesn't exist
    // ... all other task states are broken
  }
  ```
- **BROKEN**: Pool tokens reference non-existent colors:
  ```typescript
  pool: {
    bar: {
      openSlots: { value: "{colors.success.solid}" },      // ❌ "success" doesn't exist
      runningSlots: { value: "{colors.running.solid}" },  // ❌ "running" doesn't exist
      queuedSlots: { value: "{colors.queued.solid}" },     // ❌ "queued" doesn't exist
      // ... all other pool slots are broken
    }
  }
  ```
- **BROKEN**: Calendar tokens reference non-existent colors:
  ```typescript
  calendar: {
    planned: { value: { _dark: "{colors.scheduled.600}", _light: "{colors.scheduled.200}" } }, // ❌ "scheduled" doesn't exist
  }
  ```

### 🔧 What Needs Fixing
- Replace all broken task state color references with actual base colors
- Replace all broken pool color references with actual base colors  
- Replace all broken calendar color references with actual base colors
- Chart components need CSS value conversion functions (already identified)
- Some components still use hard-coded color values (already identified)

## Proposed Semantic Token Structure

### 1. Core Semantic Token Categories

#### A. Brand Tokens
```typescript
brand: generateSemanticTokens("brand")
```

#### B. Status Tokens
```typescript
status: {
  success: generateSemanticTokens("green"),
  warning: generateSemanticTokens("yellow"),
  error: generateSemanticTokens("red"),
  info: generateSemanticTokens("blue"),
  pending: generateSemanticTokens("gray")
}
```

#### C. Task State Tokens
```typescript
taskState: {
  success: generateSemanticTokens("green"),
  failed: generateSemanticTokens("red"),
  running: generateSemanticTokens("cyan"),
  queued: generateSemanticTokens("yellow"),
  scheduled: generateSemanticTokens("indigo"),
  skipped: generateSemanticTokens("gray"),
  up_for_retry: generateSemanticTokens("orange"),
  up_for_reschedule: generateSemanticTokens("purple"),
  upstream_failed: generateSemanticTokens("rose"),
  restarting: generateSemanticTokens("cyan"),
  deferred: generateSemanticTokens("amber"),
  removed: generateSemanticTokens("slate"),
  none: generateSemanticTokens("neutral")
}
```

### 2. Component-Specific Semantic Tokens

#### A. Navigation Tokens
```typescript
navigation: {
  sidebar: {
    bg: { value: { _light: "{colors.gray.50}", _dark: "{colors.gray.900}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.700}" } },
    item: {
      active: {
        bg: { value: { _light: "{colors.brand.100}", _dark: "{colors.brand.800}" } },
        text: { value: { _light: "{colors.brand.800}", _dark: "{colors.brand.200}" } }
      },
      inactive: {
        text: { value: { _light: "{colors.gray.600}", _dark: "{colors.gray.400}" } }
      }
    }
  },
  breadcrumb: {
    text: { value: { _light: "{colors.gray.600}", _dark: "{colors.gray.400}" } },
    active: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } }
  }
}
```

#### B. Data Table Tokens
```typescript
dataTable: {
  header: {
    bg: { value: { _light: "{colors.gray.50}", _dark: "{colors.gray.800}" } },
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.700}" } }
  },
  row: {
    bg: { value: { _light: "white", _dark: "{colors.gray.900}" } },
    hover: { value: { _light: "{colors.gray.50}", _dark: "{colors.gray.800}" } },
    border: { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.800}" } }
  },
  cell: {
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    muted: { value: { _light: "{colors.gray.500}", _dark: "{colors.gray.500}" } }
  }
}
```

#### C. Graph Visualization Tokens
```typescript
graph: {
  node: {
    success: { value: "{colors.green.500}" },
    failed: { value: "{colors.red.500}" },
    running: { value: "{colors.cyan.500}" },
    queued: { value: "{colors.yellow.500}" },
    skipped: { value: "{colors.gray.400}" },
    default: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } }
  },
  edge: {
    default: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } },
    active: { value: "{colors.brand.600}" }
  },
  background: {
    value: { _light: "{colors.gray.50}", _dark: "{colors.gray.900}" }
  }
}
```

#### D. Form Tokens
```typescript
form: {
  input: {
    bg: { value: { _light: "white", _dark: "{colors.gray.800}" } },
    border: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } },
    focus: { value: "{colors.brand.600}" },
    error: { value: "{colors.red.500}" },
    text: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } },
    placeholder: { value: { _light: "{colors.gray.500}", _dark: "{colors.gray.500}" } }
  },
  label: {
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    required: { value: "{colors.red.500}" }
  },
  help: {
    text: { value: { _light: "{colors.gray.600}", _dark: "{colors.gray.400}" } }
  }
}
```

#### E. Alert Tokens
```typescript
alert: {
  success: {
    bg: { value: { _light: "{colors.green.50}", _dark: "{colors.green.900/20}" } },
    border: { value: { _light: "{colors.green.200}", _dark: "{colors.green.800/40}" } },
    text: { value: { _light: "{colors.green.700}", _dark: "{colors.green.300}" } },
    icon: { value: "{colors.green.500}" }
  },
  error: {
    bg: { value: { _light: "{colors.red.50}", _dark: "{colors.red.900/20}" } },
    border: { value: { _light: "{colors.red.200}", _dark: "{colors.red.800/40}" } },
    text: { value: { _light: "{colors.red.700}", _dark: "{colors.red.300}" } },
    icon: { value: "{colors.red.500}" }
  },
  warning: {
    bg: { value: { _light: "{colors.yellow.50}", _dark: "{colors.yellow.900/20}" } },
    border: { value: { _light: "{colors.yellow.200}", _dark: "{colors.yellow.800/40}" } },
    text: { value: { _light: "{colors.yellow.700}", _dark: "{colors.yellow.300}" } },
    icon: { value: "{colors.yellow.500}" }
  },
  info: {
    bg: { value: { _light: "{colors.blue.50}", _dark: "{colors.blue.900/20}" } },
    border: { value: { _light: "{colors.blue.200}", _dark: "{colors.blue.800/40}" } },
    text: { value: { _light: "{colors.blue.700}", _dark: "{colors.blue.300}" } },
    icon: { value: "{colors.blue.500}" }
  }
}
```

#### F. Search Bar Tokens
```typescript
searchBar: {
  container: {
    bg: { value: { _light: "white", _dark: "{colors.gray.800}" } },
    border: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } },
    focus: { value: "{colors.brand.600}" }
  },
  input: {
    bg: { value: "transparent" },
    text: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } },
    placeholder: { value: { _light: "{colors.gray.500}", _dark: "{colors.gray.500}" } }
  },
  button: {
    bg: { value: "transparent" },
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    hover: { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.700}" } }
  },
  kbd: {
    bg: { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.700}" } },
    text: { value: { _light: "{colors.gray.600}", _dark: "{colors.gray.400}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.600}" } }
  }
}
```

#### G. Code Editor Tokens
```typescript
codeEditor: {
  container: {
    bg: { value: { _light: "white", _dark: "{colors.gray.900}" } },
    border: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } },
    focus: { value: "{colors.brand.600}" }
  },
  lineNumbers: {
    bg: { value: { _light: "{colors.gray.50}", _dark: "{colors.gray.800}" } },
    text: { value: { _light: "{colors.gray.500}", _dark: "{colors.gray.500}" } }
  },
  selection: {
    bg: { value: { _light: "{colors.brand.100}", _dark: "{colors.brand.800/30}" } }
  },
  syntax: {
    keyword: { value: { _light: "{colors.purple.600}", _dark: "{colors.purple.400}" } },
    string: { value: { _light: "{colors.green.600}", _dark: "{colors.green.400}" } },
    number: { value: { _light: "{colors.blue.600}", _dark: "{colors.blue.400}" } },
    comment: { value: { _light: "{colors.gray.500}", _dark: "{colors.gray.500}" } }
  }
}
```

#### H. Action Button Tokens
```typescript
actionButton: {
  primary: {
    bg: { value: "{colors.brand.600}" },
    text: { value: "white" },
    hover: { value: "{colors.brand.700}" },
    focus: { value: "{colors.brand.600}" }
  },
  secondary: {
    bg: { value: "transparent" },
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    border: { value: { _light: "{colors.gray.300}", _dark: "{colors.gray.600}" } },
    hover: { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.700}" } }
  },
  ghost: {
    bg: { value: "transparent" },
    text: { value: { _light: "{colors.gray.700}", _dark: "{colors.gray.300}" } },
    hover: { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.700}" } }
  },
  danger: {
    bg: { value: "{colors.red.600}" },
    text: { value: "white" },
    hover: { value: "{colors.red.700}" }
  }
}
```

#### I. Card Tokens
```typescript
card: {
  default: {
    bg: { value: { _light: "white", _dark: "{colors.gray.800}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.700}" } },
    shadow: { value: { _light: "0 1px 3px rgba(0, 0, 0, 0.1)", _dark: "0 1px 3px rgba(0, 0, 0, 0.3)" } }
  },
  elevated: {
    bg: { value: { _light: "white", _dark: "{colors.gray.800}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.700}" } },
    shadow: { value: { _light: "0 4px 6px rgba(0, 0, 0, 0.1)", _dark: "0 4px 6px rgba(0, 0, 0, 0.3)" } }
  },
  header: {
    bg: { value: { _light: "{colors.gray.50}", _dark: "{colors.gray.700}" } },
    text: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } },
    border: { value: { _light: "{colors.gray.200}", _dark: "{colors.gray.600}" } }
  }
}
```

#### J. Tooltip Tokens
```typescript
tooltip: {
  container: {
    bg: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } },
    text: { value: { _light: "white", _dark: "{colors.gray.900}" } },
    border: { value: "transparent" },
    shadow: { value: "0 4px 6px rgba(0, 0, 0, 0.1)" }
  },
  arrow: {
    bg: { value: { _light: "{colors.gray.900}", _dark: "{colors.gray.100}" } }
  }
}
```

### 3. Utility Functions

#### A. Existing Semantic Token Generator
**Reference**: `generateSemanticTokens()` function in `src/theme.ts` (lines 26-34)
- Leverages Chakra's color mode system with `_light` and `_dark` values
- Generates consistent semantic tokens (solid, contrast, fg, muted, subtle, emphasized, focusRing)
- Already used throughout the theme for brand colors and other semantic tokens

#### B. Task State Color Generator
**Reference**: `generateTaskStateTokens()` function in `src/theme.ts` (lines 216-244)
- Maps task states to appropriate colors (success→green, failed→red, running→cyan, etc.)
- Uses the existing `generateSemanticTokens()` function for consistency
- Already defined but needs to be used to fix broken task state tokens

#### C. Existing Token Conversion Functions
**Reference**: `getComputedCSSVariableValue()` function in `src/theme.ts` (lines 510-513)
- Converts CSS variables to their computed values
- Used for components that need actual CSS color values
- Handles Chakra UI CSS variable format (`var(--chakra-colors-...)`)

**Reference**: `getReactFlowThemeStyle()` function in `src/theme.ts` (lines 532-545)
- Returns ReactFlow style props using Chakra UI semantic tokens
- Automatically handles light/dark mode switching
- Used by Graph components (see `src/layouts/Details/Graph/Graph.tsx` and `src/pages/Asset/AssetGraph.tsx`)

#### D. New Chart Helper Function (Add to theme.ts)
```typescript
// Helper function specifically for chart components that need CSS values
export const getChartColors = (): Record<string, string> => {
  return {
    success: getComputedCSSVariableValue('var(--chakra-colors-taskState-success-solid)'),
    failed: getComputedCSSVariableValue('var(--chakra-colors-taskState-failed-solid)'),
    running: getComputedCSSVariableValue('var(--chakra-colors-taskState-running-solid)'),
    queued: getComputedCSSVariableValue('var(--chakra-colors-taskState-queued-solid)'),
    scheduled: getComputedCSSVariableValue('var(--chakra-colors-taskState-scheduled-solid)'),
    skipped: getComputedCSSVariableValue('var(--chakra-colors-taskState-skipped-solid)'),
    up_for_retry: getComputedCSSVariableValue('var(--chakra-colors-taskState-up_for_retry-solid)'),
    up_for_reschedule: getComputedCSSVariableValue('var(--chakra-colors-taskState-up_for_reschedule-solid)'),
    upstream_failed: getComputedCSSVariableValue('var(--chakra-colors-taskState-upstream_failed-solid)'),
    restarting: getComputedCSSVariableValue('var(--chakra-colors-taskState-restarting-solid)'),
    deferred: getComputedCSSVariableValue('var(--chakra-colors-taskState-deferred-solid)'),
    removed: getComputedCSSVariableValue('var(--chakra-colors-taskState-removed-solid)'),
    none: getComputedCSSVariableValue('var(--chakra-colors-taskState-none-solid)')
  };
};
```


## Implementation Recommendations

### 1. Surgical Fix Strategy

#### Phase 1: Fix Existing Theme Issues (theme.ts only)

**A. Fix Broken Task State Tokens**
Replace all broken task state color references with actual base colors:

```typescript
// BEFORE (BROKEN):
taskState: {
  success: generateSemanticTokens("success"),        // ❌ "success" doesn't exist
  failed: generateSemanticTokens("failed"),         // ❌ "failed" doesn't exist
  running: generateSemanticTokens("running"),       // ❌ "running" doesn't exist
  queued: generateSemanticTokens("queued"),          // ❌ "queued" doesn't exist
  scheduled: generateSemanticTokens("scheduled"),   // ❌ "scheduled" doesn't exist
  skipped: generateSemanticTokens("skipped"),       // ❌ "skipped" doesn't exist
  up_for_retry: generateSemanticTokens("up_for_retry"), // ❌ "up_for_retry" doesn't exist
  up_for_reschedule: generateSemanticTokens("up_for_reschedule"), // ❌ "up_for_reschedule" doesn't exist
  upstream_failed: generateSemanticTokens("upstream_failed"), // ❌ "upstream_failed" doesn't exist
  restarting: generateSemanticTokens("restarting"), // ❌ "restarting" doesn't exist
  deferred: generateSemanticTokens("deferred"),      // ❌ "deferred" doesn't exist
  removed: generateSemanticTokens("removed"),        // ❌ "removed" doesn't exist
  none: generateSemanticTokens("none"),             // ❌ "none" doesn't exist
}

// AFTER (FIXED):
taskState: {
  success: generateSemanticTokens("green"),         // ✅ "green" exists
  failed: generateSemanticTokens("red"),            // ✅ "red" exists
         running: generateSemanticTokens("cyan"),          // ✅ "cyan" exists
  queued: generateSemanticTokens("yellow"),         // ✅ "yellow" exists
  scheduled: generateSemanticTokens("indigo"),      // ✅ "indigo" exists
  skipped: generateSemanticTokens("gray"),          // ✅ "gray" exists
  up_for_retry: generateSemanticTokens("orange"),    // ✅ "orange" exists
  up_for_reschedule: generateSemanticTokens("purple"), // ✅ "purple" exists
  upstream_failed: generateSemanticTokens("rose"),   // ✅ "rose" exists
  restarting: generateSemanticTokens("cyan"),       // ✅ "cyan" exists
  deferred: generateSemanticTokens("amber"),        // ✅ "amber" exists
  removed: generateSemanticTokens("slate"),         // ✅ "slate" exists
  none: generateSemanticTokens("neutral"),          // ✅ "neutral" exists
}
```

**B. Fix Broken Pool Tokens**
Replace all broken pool color references:

```typescript
// BEFORE (BROKEN):
pool: {
  bar: {
    openSlots: { value: "{colors.success.solid}" },      // ❌ "success" doesn't exist
    runningSlots: { value: "{colors.running.solid}" },  // ❌ "running" doesn't exist
    queuedSlots: { value: "{colors.queued.solid}" },     // ❌ "queued" doesn't exist
    scheduledSlots: { value: "{colors.scheduled.solid}" }, // ❌ "scheduled" doesn't exist
    deferredSlots: { value: "{colors.deferred.solid}" }, // ❌ "deferred" doesn't exist
  }
}

// AFTER (FIXED):
pool: {
  bar: {
    openSlots: { value: "{colors.green.600}" },        // ✅ "green" exists
    runningSlots: { value: "{colors.cyan.600}" },     // ✅ "cyan" exists
    queuedSlots: { value: "{colors.yellow.600}" },     // ✅ "yellow" exists
    scheduledSlots: { value: "{colors.indigo.600}" },  // ✅ "indigo" exists
    deferredSlots: { value: "{colors.amber.600}" },    // ✅ "amber" exists
  }
}
```

**C. Fix Broken Calendar Tokens**
Replace all broken calendar color references:

```typescript
// BEFORE (BROKEN):
calendar: {
  planned: { value: { _dark: "{colors.scheduled.600}", _light: "{colors.scheduled.200}" } }, // ❌ "scheduled" doesn't exist
}

// AFTER (FIXED):
calendar: {
  planned: { value: { _dark: "{colors.indigo.600}", _light: "{colors.indigo.200}" } }, // ✅ "indigo" exists
}
```

**D. Add Chart Helper Function**
Add `getChartColors()` helper function to `theme.ts` (uses existing `getComputedCSSVariableValue`)

#### Phase 2: Targeted Component Updates
- Identify components with hard-coded colors (charts, graphs, SVGs)
- Update only the specific components that need changes
- Use conversion functions for non-Chakra components
- Use direct semantic tokens for Chakra components
- Test each component individually

#### Phase 3: Validation & Testing
- Test all updated components in both light and dark modes
- Verify visual consistency across components
- Ensure no regressions in existing functionality
- Validate that chart components work correctly with fixed token system

### 2. Surgical Implementation Approach

**Primary Changes:**
- **`src/theme.ts`** - Add semantic tokens and conversion functions
- **Impacted Components** - Update only components that need changes

**No New Files Required:**
- All semantic tokens defined in existing `theme.ts`
- Conversion functions added to `theme.ts`
- Component-specific tokens organized within `theme.ts`

### 3. Usage Examples

#### A. Standard Chakra Components (Direct Token Usage)
```typescript
// Before (Current Approach)
<Box bg="gray.50" color="gray.700" borderColor="gray.200">
  <Text color="red.500">Failed Task</Text>
</Box>

// After (Semantic Tokens)
<Box bg="card.default.bg" color="card.default.text" borderColor="card.default.border">
  <Text color="taskState.failed.text">Failed Task</Text>
</Box>
```

#### B. Chart Components (Using Existing Functions)
```typescript
// Duration Chart Component
import { getChartColors } from '../theme'; // Import from theme.ts

const DurationChart = () => {
  const chartColors = getChartColors();
  
  const chartConfig = {
    colors: {
      success: chartColors.success,      // Green color in light mode
      failed: chartColors.failed,        // Red color in light mode
      running: chartColors.running,      // Blue color in light mode
      queued: chartColors.queued,        // Yellow color in light mode
      scheduled: chartColors.scheduled,  // Indigo color in light mode
      skipped: chartColors.skipped,      // Gray color in light mode
      up_for_retry: chartColors.up_for_retry,        // Orange color in light mode
      up_for_reschedule: chartColors.up_for_reschedule, // Purple color in light mode
      upstream_failed: chartColors.upstream_failed,   // Rose color in light mode
      restarting: chartColors.restarting,            // Cyan color in light mode
      deferred: chartColors.deferred,                // Amber color in light mode
      removed: chartColors.removed,                  // Slate color in light mode
      none: chartColors.none,                        // Neutral color in light mode
    }
  };
  
  return <ChartComponent config={chartConfig} />;
};

// ReactFlow Graph Component (Uses existing function)
import { getReactFlowThemeStyle } from '../theme';

const GraphComponent = () => {
  return (
    <ReactFlow
      style={getReactFlowThemeStyle()} // Automatically handles light/dark mode
      nodes={nodes}
      edges={edges}
    />
  );
};

// SVG Components (Uses existing function)
import { getComputedCSSVariableValue } from '../theme';

const TaskNode = ({ state }: { state: string }) => {
  const fillColor = getComputedCSSVariableValue(`var(--chakra-colors-taskState-${state}-solid)`);
  
  return (
    <svg>
      <circle fill={fillColor} />
    </svg>
  );
};
```

#### C. Pool Components (Semantic Token Usage)
```typescript
// Pool Bar Component
const PoolBar = ({ openSlots, runningSlots, queuedSlots }) => {
  return (
    <Box>
      <Box bg="pool.bar.openSlots" />      {/* Semantic token */}
      <Box bg="pool.bar.runningSlots" />   {/* Semantic token */}
      <Box bg="pool.bar.queuedSlots" />    {/* Semantic token */}
    </Box>
  );
};
```

#### D. Calendar Components (Semantic Token Usage)
```typescript
// Calendar Component
const Calendar = () => {
  return (
    <Box>
      <Box bg="calendar.planned" />        {/* Semantic token */}
      <Box bg="calendar.totalRuns.level1" /> {/* Semantic token */}
      <Box bg="calendar.failedRuns.level1" /> {/* Semantic token */}
    </Box>
  );
};
```

#### E. Mixed Component Usage
```typescript
const TaskInstanceCard = ({ taskInstance }) => {
  const stateColor = getComputedCSSVariableValue(`var(--chakra-colors-taskState-${taskInstance.state}-solid)`);
  
  return (
    <Box 
      bg="card.default.bg" 
      borderColor="card.default.border"
      borderWidth="1px"
    >
      <HStack>
        <Box
          width="4"
          height="4"
          borderRadius="full"
          bg={stateColor} // CSS value for non-Chakra component
        />
        <Text color="taskState.failed.text">{taskInstance.taskId}</Text>
      </HStack>
    </Box>
  );
};
```

### 4. Components Requiring CSS Value Conversion

Certain components cannot use semantic tokens directly and must leverage the conversion function to get CSS values:

#### A. Chart Libraries
- **Duration Chart**: Uses task state colors for bar charts
- **Gantt Chart**: Requires task state colors for timeline visualization
- **Trend Charts**: Uses status colors for trend lines
- **Pie Charts**: Uses task state colors for segments

#### B. SVG Components
- **Graph Nodes**: Task state colors for node fill
- **Graph Edges**: Connection colors and states
- **Icons**: Custom SVG icons with dynamic colors
- **Progress Indicators**: Custom progress visualizations

#### C. Third-Party Libraries
- **D3.js Visualizations**: Require CSS color values
- **Chart.js**: Needs actual color values, not tokens
- **React Flow**: Custom node and edge styling
- **Timeline Libraries**: Task state color mapping

#### D. Canvas Components
- **Custom Drawings**: Canvas-based visualizations
- **Image Processing**: Dynamic color overlays
- **Custom Charts**: Canvas-based chart implementations

### 5. Benefits

#### A. Consistency
- All components use the same semantic meaning for colors
- Consistent behavior across light and dark modes
- Unified design language throughout the application
- Chart components maintain visual consistency with UI components

#### B. Maintainability
- Single source of truth for design decisions
- Easy to update brand colors globally
- Clear semantic meaning for each token
- Centralized color management for both UI and chart components

#### C. Scalability
- Easy to add new themes or color variations
- Simple to extend with new component-specific tokens
- Supports future design system evolution
- Seamless integration with new chart libraries

#### D. Developer Experience
- IntelliSense support for semantic tokens
- Clear naming conventions
- Reduced cognitive load when choosing colors
- Unified approach for both UI and chart components

### 6. Surgical Fix Checklist

**Phase 1: Fix Existing Theme Issues (theme.ts only)**
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("success")` with `generateSemanticTokens("green")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("failed")` with `generateSemanticTokens("red")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("running")` with `generateSemanticTokens("cyan")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("queued")` with `generateSemanticTokens("yellow")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("scheduled")` with `generateSemanticTokens("indigo")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("skipped")` with `generateSemanticTokens("gray")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("up_for_retry")` with `generateSemanticTokens("orange")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("up_for_reschedule")` with `generateSemanticTokens("purple")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("upstream_failed")` with `generateSemanticTokens("rose")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("restarting")` with `generateSemanticTokens("cyan")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("deferred")` with `generateSemanticTokens("amber")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("removed")` with `generateSemanticTokens("slate")`
- [ ] **Fix broken task state tokens** - Replace `generateSemanticTokens("none")` with `generateSemanticTokens("neutral")`
- [ ] **Fix broken pool tokens** - Replace `{colors.success.solid}` with `{colors.green.solid}`
- [ ] **Fix broken pool tokens** - Replace `{colors.running.solid}` with `{colors.cyan.solid}`
- [ ] **Fix broken pool tokens** - Replace `{colors.queued.solid}` with `{colors.yellow.solid}`
- [ ] **Fix broken pool tokens** - Replace `{colors.scheduled.solid}` with `{colors.indigo.solid}`
- [ ] **Fix broken pool tokens** - Replace `{colors.deferred.solid}` with `{colors.amber.solid}`
- [ ] **Fix broken calendar tokens** - Replace `{colors.scheduled.600}` with `{colors.indigo.600}`
- [ ] **Fix broken calendar tokens** - Replace `{colors.scheduled.200}` with `{colors.indigo.200}`
- [ ] Add `getChartColors()` helper function to `theme.ts` (uses existing `getComputedCSSVariableValue`)
- [ ] Add comprehensive component tokens for SearchBar, CodeEditor, ActionButton, Card, Tooltip
- [ ] Fix any other broken semantic token references

**Phase 1.5: Add User Configuration Support (theme.ts only)**
- [ ] Add `SemanticTokenConfig` interface for user overrides
- [ ] Create `createCustomConfig()` function that accepts user configuration
- [ ] Add configuration loading methods (env, file, runtime)
- [ ] Add `loadSemanticTokenConfig()` function for easy configuration loading
- [ ] Support CSS variable overrides for simple customization

**Phase 2: Component Updates (surgical changes only)**
- [ ] Update Duration Chart component to use `getChartColors()`
- [ ] Update Gantt Chart component to use `getComputedCSSVariableValue()`
- [ ] Update Graph visualization components to use `getReactFlowThemeStyle()` (already implemented)
- [ ] Update any SVG components with hard-coded colors to use `getComputedCSSVariableValue()`
- [ ] Update components that reference broken semantic tokens

**Phase 3: Testing & Validation**
- [ ] Test all updated components in light mode
- [ ] Test all updated components in dark mode
- [ ] Verify chart components maintain visual consistency
- [ ] Validate no regressions in existing functionality
- [ ] Verify task state colors display correctly

**No New Files Required:**
- ✅ All changes contained within existing `theme.ts`
- ✅ Only update components that actually need changes
- ✅ Import conversion functions directly from `theme.ts`
- ✅ Maintain existing file structure

### 6. User-Configurable Semantic Tokens

#### A. Configuration Structure
```typescript
// User-configurable semantic token overrides
export interface SemanticTokenConfig {
  // Brand color overrides
  brand?: {
    primary?: string;    // Override brand.600
    secondary?: string;  // Override brand.400
    accent?: string;     // Override brand.500
  };
  
  // Task state color overrides
  taskStates?: {
    success?: string;    // Override green.600
    failed?: string;     // Override red.600
    running?: string;    // Override cyan.600
    queued?: string;     // Override yellow.600
    // ... other task states
  };
  
  // Component-specific overrides
  components?: {
    dataTable?: {
      headerBg?: string;
      rowBg?: string;
      borderColor?: string;
    };
    navigation?: {
      sidebarBg?: string;
      activeItemBg?: string;
    };
    // ... other components
  };
}
```

#### B. Configuration Loading
```typescript
// Enhanced theme.ts with user configuration support
export const createCustomConfig = (userConfig?: SemanticTokenConfig) => {
  const baseConfig = defineConfig({
    theme: {
      tokens: {
        colors: {
          // Base color palette (unchanged)
          brand: { /* existing brand colors */ },
          green: { /* existing green colors */ },
          red: { /* existing red colors */ },
          // ... other base colors
        },
      },
      semanticTokens: {
        colors: {
          // Apply user overrides to semantic tokens
          brand: generateSemanticTokens(
            userConfig?.brand?.primary ? 'custom-brand' : 'brand'
          ),
          taskState: {
            success: generateSemanticTokens(
              userConfig?.taskStates?.success ? 'custom-success' : 'green'
            ),
            failed: generateSemanticTokens(
              userConfig?.taskStates?.failed ? 'custom-failed' : 'red'
            ),
            // ... other task states
          },
          // Component-specific overrides
          dataTable: {
            header: {
              bg: userConfig?.components?.dataTable?.headerBg 
                ? { value: userConfig.components.dataTable.headerBg }
                : { value: { _light: "{colors.gray.100}", _dark: "{colors.gray.900}" } },
            },
            // ... other dataTable tokens
          },
        },
      },
    },
  });
  
  return baseConfig;
};
```

#### C. User Configuration Examples
```typescript
// Example 1: Brand color override
const customBrandConfig: SemanticTokenConfig = {
  brand: {
    primary: '#FF6B35',    // Custom orange brand color
    secondary: '#F7931E',  // Custom orange secondary
  }
};

// Example 2: Task state color override
const customTaskStateConfig: SemanticTokenConfig = {
  taskStates: {
    success: '#00C851',    // Custom green for success
    failed: '#FF4444',     // Custom red for failed
    running: '#00BCD4',    // Custom cyan for running
    queued: '#FFA500',     // Custom orange for queued
    scheduled: '#8A2BE2', // Custom purple for scheduled
    skipped: '#808080',    // Custom gray for skipped
  }
};

// Example 3: Component-specific override
const customComponentConfig: SemanticTokenConfig = {
  components: {
    dataTable: {
      headerBg: '#F5F5F5',     // Custom header background
      rowBg: '#FFFFFF',        // Custom row background
      borderColor: '#E0E0E0',  // Custom border color
    },
    navigation: {
      sidebarBg: '#2C3E50',    // Custom dark sidebar
      activeItemBg: '#34495E', // Custom active item background
    },
  }
};
```

#### D. Configuration Loading Methods
```typescript
// Method 1: Environment variable
const loadConfigFromEnv = (): SemanticTokenConfig | undefined => {
  const configJson = process.env.REACT_APP_SEMANTIC_TOKENS_CONFIG;
  return configJson ? JSON.parse(configJson) : undefined;
};

// Method 2: Configuration file
const loadConfigFromFile = async (): Promise<SemanticTokenConfig | undefined> => {
  try {
    const response = await fetch('/config/semantic-tokens.json');
    return response.ok ? await response.json() : undefined;
  } catch {
    return undefined;
  }
};

// Method 3: Runtime configuration
const loadConfigFromRuntime = (): SemanticTokenConfig | undefined => {
  return (window as any).SEMANTIC_TOKENS_CONFIG;
};

// Main configuration loader
export const loadSemanticTokenConfig = async (): Promise<SemanticTokenConfig | undefined> => {
  // Try different loading methods in order of preference
  return loadConfigFromRuntime() || 
         loadConfigFromEnv() || 
         await loadConfigFromFile();
};
```

#### E. Easy Override Examples
```typescript
// Simple CSS variable override (easiest for users)
:root {
  --chakra-colors-taskState-success-solid: #00C851;
  --chakra-colors-taskState-failed-solid: #FF4444;
  --chakra-colors-taskState-running-solid: #2196F3;
  --chakra-colors-taskState-queued-solid: #FFA500;
  --chakra-colors-taskState-scheduled-solid: #8A2BE2;
  --chakra-colors-taskState-skipped-solid: #808080;
  --chakra-colors-brand-solid: #FF6B35;
}

// JSON configuration file (semantic-tokens.json)
{
  "taskStates": {
    "success": "#00C851",
    "failed": "#FF4444",
    "running": "#2196F3",
    "queued": "#FFA500",
    "scheduled": "#8A2BE2",
    "skipped": "#808080"
  },
  "brand": {
    "primary": "#FF6B35"
  }
}

// Environment variable override
REACT_APP_SEMANTIC_TOKENS_CONFIG='{"taskStates":{"success":"#00C851","failed":"#FF4444","running":"#2196F3","queued":"#FFA500"}}'
```

### 7. Future Considerations

#### A. Advanced Configuration Features
- **Theme presets**: Pre-defined configurations for common use cases
- **Dynamic switching**: Runtime theme switching without page reload
- **Validation**: Configuration validation with helpful error messages
- **Documentation**: Auto-generated documentation for available tokens

#### B. Integration Features
- **Design system sync**: Integration with Figma/Sketch for design token sync
- **A/B testing**: Support for different configurations in testing
- **Analytics**: Track which configurations are most popular

#### C. Performance Optimizations
- **Lazy loading**: Load configurations only when needed
- **Caching**: Cache computed token values for better performance
- **Tree shaking**: Remove unused token configurations

## Conclusion

Implementing a centralized semantic tokens system will significantly improve the maintainability, consistency, and scalability of the Airflow UI. The proposed structure provides a solid foundation for future design system evolution while maintaining backward compatibility during the migration process.

The semantic approach ensures that design decisions are meaningful and consistent, making it easier for developers to create cohesive user experiences and for designers to maintain brand consistency across the application.
