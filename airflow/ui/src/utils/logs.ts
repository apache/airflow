/* eslint-disable perfectionist/sort-enums */

/* eslint-disable perfectionist/sort-objects */
import { createListCollection } from "@chakra-ui/react";

export enum LogLevel {
  DEBUG = "DEBUG",
  INFO = "INFO",
  WARNING = "WARNING",
  ERROR = "ERROR",
  CRITICAL = "CRITICAL",
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
    { label: LogLevel.DEBUG, value: "debug" },
    { label: LogLevel.INFO, value: "info" },
    { label: LogLevel.WARNING, value: "warning" },
    { label: LogLevel.ERROR, value: "error" },
    { label: LogLevel.CRITICAL, value: "critical" },
  ],
});
