<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [{{PROJECT_NAME}}](#project_name)
  - [Getting Started](#getting-started)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# {{PROJECT_NAME}}

This project was bootstrapped with the Airflow UI configuration.

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- pnpm

### Installation

```bash
# Install dependencies
pnpm install

# Start development server
pnpm dev
```

### Available Scripts

- `pnpm dev` - Start development server
- `pnpm build` - Build for production
- `pnpm lint` - Run linter
- `pnpm lint:fix` - Fix linting issues
- `pnpm format` - Format code with prettier
- `pnpm test` - Run tests
- `pnpm coverage` - Run tests with coverage

### Tech Stack

- **React 18** - UI library
- **TypeScript** - Type safety
- **Vite** - Build tool
- **Chakra UI** - Component library
- **React Query** - Data fetching
- **React Router** - Routing
- **Vitest** - Testing framework
- **ESLint** - Code linting
- **Prettier** - Code formatting

### Project Structure

```
src/
├── components/     # Reusable UI components
├── pages/         # Page components
├── hooks/         # Custom React hooks
├── context/       # React context providers
├── utils/         # Utility functions
├── constants/     # Constants and configuration
└── assets/        # Static assets
```

### Development

The project uses the same configuration as Airflow's core UI:

- **ESLint** for code quality
- **Prettier** for consistent formatting
- **TypeScript** for type safety
- **Chakra UI** for consistent design system
- **React Query** for efficient data fetching

### Customization

- Modify `src/theme.ts` to customize the Chakra UI theme
- Add new routes in `src/router.tsx`
- Add new pages in `src/pages/`
- Add reusable components in `src/components/`
