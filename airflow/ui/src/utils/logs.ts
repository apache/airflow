/* eslint-disable perfectionist/sort-enums */

/* eslint-disable perfectionist/sort-objects */
import { createListCollection } from "@chakra-ui/react";

export enum LogLevel {
  DEBUG = "debug",
  INFO = "info",
  WARNING = "warning",
  ERROR = "error",
  CRITICAL = "critical",
}

export const logLevelColorMapping = {
  [LogLevel.DEBUG]: "gray",
  [LogLevel.INFO]: "green",
  [LogLevel.WARNING]: "yellow",
  [LogLevel.ERROR]: "orange",
  [LogLevel.CRITICAL]: "red",
};

export const logLevelOptions = createListCollection<{
  label: string;
  value: string;
}>({
  items: [
    { label: "All Levels", value: "all" },
    { label: LogLevel.DEBUG.toUpperCase(), value: LogLevel.DEBUG },
    { label: LogLevel.INFO.toUpperCase(), value: LogLevel.INFO },
    { label: LogLevel.WARNING.toUpperCase(), value: LogLevel.WARNING },
    { label: LogLevel.ERROR.toUpperCase(), value: LogLevel.WARNING },
    { label: LogLevel.CRITICAL.toUpperCase(), value: LogLevel.CRITICAL },
  ],
});
