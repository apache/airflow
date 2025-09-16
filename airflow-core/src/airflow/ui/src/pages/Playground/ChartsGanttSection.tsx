/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/* eslint-disable @typescript-eslint/no-explicit-any */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */

/* eslint-disable @typescript-eslint/no-unsafe-member-access */

/* eslint-disable max-lines */

/* eslint-disable perfectionist/sort-objects */

/* eslint-disable perfectionist/sort-jsx-props */

/* eslint-disable @stylistic/padding-line-between-statements */

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Box, Collapsible, Heading, HStack, Text, VStack, useToken } from "@chakra-ui/react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Title,
  Tooltip,
  Legend,
} from "chart.js";
import { Bar, Line } from "react-chartjs-2";

import { getComputedCSSVariableValue } from "src/theme";

// import { useColorMode } from "src/context/colorMode";

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Title,
  Tooltip,
  Legend,
);

// Helper function to add alpha transparency to any color format
const addAlpha = (color: string, alpha: number = 0.2) => {
  // If it's already a hex color, add alpha
  if (color.startsWith("#")) {
    const hex =
      alpha === 1
        ? "FF"
        : Math.round(alpha * 255)
            .toString(16)
            .padStart(2, "0");
    return color + hex;
  }
  // For rgb/hsl colors, use CSS color-mix for transparency
  return `color-mix(in srgb, ${color} ${Math.round(alpha * 100)}%, transparent)`;
};

type ChartsGanttSectionProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const ChartsGanttSection = ({ isOpen, onToggle }: ChartsGanttSectionProps) => {
  // const { colorMode } = useColorMode();

  // Get semantic token references
  const [
    successToken,
    failedToken,
    runningToken,
    queuedToken,
    skippedToken,
    upstreamFailedToken,
    upForRetryToken,
    upForRescheduleToken,
    scheduledToken,
    deferredToken,
    removedToken,
  ] = useToken("colors", [
    "success.solid",
    "failed.solid",
    "running.solid",
    "queued.solid",
    "skipped.solid",
    "upstream_failed.solid",
    "up_for_retry.solid",
    "up_for_reschedule.solid",
    "scheduled.solid",
    "deferred.solid",
    "removed.solid",
  ]);

  // Convert CSS variables to computed color values
  const success = getComputedCSSVariableValue(successToken ?? "oklch(0.5 0 0)");
  const failed = getComputedCSSVariableValue(failedToken ?? "oklch(0.5 0 0)");
  const running = getComputedCSSVariableValue(runningToken ?? "oklch(0.5 0 0)");
  const queued = getComputedCSSVariableValue(queuedToken ?? "oklch(0.5 0 0)");
  const skipped = getComputedCSSVariableValue(skippedToken ?? "oklch(0.5 0 0)");
  const upstreamFailed = getComputedCSSVariableValue(upstreamFailedToken ?? "oklch(0.5 0 0)");
  const upForRetry = getComputedCSSVariableValue(upForRetryToken ?? "oklch(0.5 0 0)");
  const upForReschedule = getComputedCSSVariableValue(upForRescheduleToken ?? "oklch(0.5 0 0)");
  const scheduled = getComputedCSSVariableValue(scheduledToken ?? "oklch(0.5 0 0)");
  const deferred = getComputedCSSVariableValue(deferredToken ?? "oklch(0.5 0 0)");
  const removed = getComputedCSSVariableValue(removedToken ?? "oklch(0.5 0 0)");

  // Mock data for duration chart
  const durationData = {
    labels: [
      "success_task",
      "failed_task",
      "running_task",
      "queued_task",
      "skipped_task",
      "upstream_failed_task",
      "up_for_retry_task",
      "up_for_reschedule_task",
      "scheduled_task",
      "deferred_task",
      "removed_task",
    ],
    datasets: [
      {
        label: "Duration (minutes)",
        data: [12, 8, 15, 6, 20, 4, 9, 3, 18, 11, 1],
        backgroundColor: [
          success,
          failed,
          running,
          queued,
          skipped,
          upstreamFailed,
          upForRetry,
          upForReschedule,
          scheduled,
          deferred,
          removed,
        ],
        borderColor: [
          success,
          failed,
          running,
          queued,
          skipped,
          upstreamFailed,
          upForRetry,
          upForReschedule,
          scheduled,
          deferred,
          removed,
        ],
        borderWidth: 1,
      },
    ],
  };

  // Mock data for trend chart
  const trendData = {
    labels: ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
    datasets: [
      {
        label: "Success Rate",
        data: [85, 88, 92, 87, 94, 91],
        borderColor: success,
        backgroundColor: addAlpha(success, 0.2),
        fill: true,
        tension: 0.4,
      },
      {
        label: "Failure Rate",
        data: [8, 7, 5, 9, 4, 6],
        borderColor: failed,
        backgroundColor: addAlpha(failed, 0.2),
        fill: true,
        tension: 0.4,
      },
      {
        label: "Running Rate",
        data: [3, 2, 1, 2, 1, 1],
        borderColor: running,
        backgroundColor: addAlpha(running, 0.2),
        fill: true,
        tension: 0.4,
      },
      {
        label: "Queued Rate",
        data: [2, 1, 1, 1, 0, 1],
        borderColor: queued,
        backgroundColor: addAlpha(queued, 0.2),
        fill: true,
        tension: 0.4,
      },
      {
        label: "Skipped Rate",
        data: [2, 2, 1, 1, 1, 1],
        borderColor: skipped,
        backgroundColor: addAlpha(skipped, 0.2),
        fill: true,
        tension: 0.4,
      },
    ],
  };

  // Mock data for Gantt-style chart
  const ganttData = {
    labels: [
      "success_task",
      "running_task",
      "failed_task",
      "queued_task",
      "skipped_task",
      "upstream_failed_task",
      "up_for_retry_task",
      "scheduled_task",
      "deferred_task",
      "removed_task",
    ],
    datasets: [
      {
        label: "Task Timeline",
        data: [
          { x: ["2024-01-01 09:00", "2024-01-01 09:15"], y: "success_task" },
          { x: ["2024-01-01 09:15", "2024-01-01 10:30"], y: "running_task" },
          { x: ["2024-01-01 10:30", "2024-01-01 11:45"], y: "failed_task" },
          { x: ["2024-01-01 11:45", "2024-01-01 12:30"], y: "queued_task" },
          { x: ["2024-01-01 12:30", "2024-01-01 12:45"], y: "skipped_task" },
          { x: ["2024-01-01 12:45", "2024-01-01 13:00"], y: "upstream_failed_task" },
          { x: ["2024-01-01 13:00", "2024-01-01 13:15"], y: "up_for_retry_task" },
          { x: ["2024-01-01 13:15", "2024-01-01 13:30"], y: "scheduled_task" },
          { x: ["2024-01-01 13:30", "2024-01-01 13:45"], y: "deferred_task" },
          { x: ["2024-01-01 13:45", "2024-01-01 13:50"], y: "removed_task" },
        ],
        backgroundColor: [
          success,
          running,
          failed,
          queued,
          skipped,
          upstreamFailed,
          upForRetry,
          scheduled,
          deferred,
          removed,
        ],
        borderColor: [
          success,
          running,
          failed,
          queued,
          skipped,
          upstreamFailed,
          upForRetry,
          scheduled,
          deferred,
          removed,
        ],
        borderWidth: 1,
        maxBarThickness: 20,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "top" as const,
      },
      title: {
        display: false,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
      },
    },
  };

  const ganttOptions = {
    indexAxis: "y" as const,
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        callbacks: {
          title: (context: any) => `Task: ${context[0].label}`,
          label: (context: any) => {
            const data = context.raw;
            return `Duration: ${data.x[0]} to ${data.x[1]}`;
          },
        },
      },
    },
    scales: {
      x: {
        type: "time" as const,
        time: {
          displayFormats: {
            hour: "HH:mm",
          },
        },
      },
    },
  };

  const lineOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "top" as const,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        max: 100,
        ticks: {
          callback: (value: any) => `${value}%`,
        },
      },
    },
  };

  return (
    <Box id="charts">
      <Collapsible.Root onOpenChange={onToggle} open={isOpen}>
        <Collapsible.Trigger
          borderWidth="1px"
          borderColor={isOpen ? "brand.emphasized" : "border.muted"}
          cursor="pointer"
          paddingX="6"
          paddingY="4"
          transition="all 0.2s"
          width="full"
          _hover={{ bg: "bg.subtle" }}
        >
          <HStack justify="space-between" width="full">
            <VStack align="flex-start" gap="1">
              <Heading size="xl">Charts & Gantt</Heading>
              <Text color="fg.muted" fontSize="sm">
                Data visualization and timeline charts
              </Text>
            </VStack>
            <Text color="brand.solid" fontSize="lg">
              {isOpen ? "−" : "+"}
            </Text>
          </HStack>
        </Collapsible.Trigger>
        <Collapsible.Content>
          <Box borderWidth="1px" borderColor="border.muted" borderTop="none" padding="6">
            <VStack align="stretch" gap={6}>
              {/* First Row: Duration and Trend Charts */}
              <HStack gap={6} align="flex-start" flexWrap="wrap">
                {/* Duration Chart */}
                <VStack align="stretch" flex="1" gap={4} minWidth="400px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Duration Chart</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Bar chart showing task durations
                    </Text>
                  </VStack>
                  <Box height="250px" width="100%">
                    <Bar data={durationData} options={chartOptions} />
                  </Box>
                  <Text color="fg.muted" fontSize="sm">
                    Displays task execution times with color-coded states. Accessible via screen reader with
                    data table fallback.
                  </Text>
                </VStack>

                {/* Trend Chart */}
                <VStack align="stretch" flex="1" gap={4} minWidth="400px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Trend Chart</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Line chart showing success/failure trends
                    </Text>
                  </VStack>
                  <Box height="250px" width="100%">
                    <Line data={trendData} options={lineOptions} />
                  </Box>
                  <Text color="fg.muted" fontSize="sm">
                    Shows task success and failure rates over time. Includes hover tooltips and keyboard
                    navigation.
                  </Text>
                </VStack>
              </HStack>

              {/* Second Row: Gantt Chart and Chart Accessibility */}
              <HStack gap={6} align="flex-start" flexWrap="wrap">
                {/* Gantt Chart */}
                <VStack align="stretch" flex="1" gap={4} minWidth="400px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Gantt Chart</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Timeline visualization of task execution
                    </Text>
                  </VStack>
                  <Box height="250px" width="100%">
                    <Bar data={ganttData} options={ganttOptions} />
                  </Box>
                  <Text color="fg.muted" fontSize="sm">
                    Horizontal bar chart showing task timelines and dependencies. Each bar represents task
                    start/end times with state colors.
                  </Text>
                </VStack>

                {/* Chart Accessibility Features */}
                <VStack align="stretch" flex="1" gap={4} minWidth="400px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Chart Accessibility</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      A11y features for charts
                    </Text>
                  </VStack>
                  <VStack align="stretch" gap={3}>
                    <Text fontSize="sm" fontWeight="semibold">
                      Keyboard Navigation:
                    </Text>
                    <VStack align="stretch" gap={2}>
                      <Text fontSize="sm">• Tab: Navigate between chart elements</Text>
                      <Text fontSize="sm">• Arrow keys: Move between data points</Text>
                      <Text fontSize="sm">• Enter: Activate/select data point</Text>
                      <Text fontSize="sm">• Escape: Exit chart focus</Text>
                    </VStack>

                    <Text fontSize="sm" fontWeight="semibold">
                      Screen Reader Support:
                    </Text>
                    <VStack align="stretch" gap={2}>
                      <Text fontSize="sm">• Chart type and purpose announced</Text>
                      <Text fontSize="sm">• Data values read aloud</Text>
                      <Text fontSize="sm">• Axis labels and scales described</Text>
                      <Text fontSize="sm">• Alternative data table available</Text>
                    </VStack>

                    <Text fontSize="sm" fontWeight="semibold">
                      Visual Accessibility:
                    </Text>
                    <VStack align="stretch" gap={2}>
                      <Text fontSize="sm">• High contrast color schemes</Text>
                      <Text fontSize="sm">• Pattern/texture options for color blindness</Text>
                      <Text fontSize="sm">• Scalable text and elements</Text>
                      <Text fontSize="sm">• Focus indicators visible</Text>
                    </VStack>
                  </VStack>
                </VStack>
              </HStack>
            </VStack>
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
