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

export type LineageStyle = "downstream" | "focus" | "upstream";

export const getLineageNodeStyle = ({
  defaultBackground,
  defaultBorderColor,
  defaultBorderWidth,
  lineageStyle,
}: {
  defaultBackground: string;
  defaultBorderColor: string;
  defaultBorderWidth: number;
  lineageStyle?: LineageStyle;
}) => {
  if (lineageStyle === "focus") {
    return {
      background: "bg.emphasized",
      borderColor: "blue.500",
      borderWidth: 4,
      boxShadow: "0 0 0 2px rgba(66, 153, 225, 0.35), 0 8px 20px rgba(66, 153, 225, 0.2)",
      transform: "translateY(-1px)",
    };
  }

  if (lineageStyle === "upstream") {
    return {
      background: "rgba(237, 137, 54, 0.10)",
      borderColor: "orange.400",
      borderWidth: 3,
      boxShadow: "0 0 0 1px rgba(237, 137, 54, 0.35)",
      transform: "none",
    };
  }

  if (lineageStyle === "downstream") {
    return {
      background: "rgba(56, 178, 172, 0.10)",
      borderColor: "teal.400",
      borderWidth: 3,
      boxShadow: "0 0 0 1px rgba(56, 178, 172, 0.35)",
      transform: "none",
    };
  }

  return {
    background: defaultBackground,
    borderColor: defaultBorderColor,
    borderWidth: defaultBorderWidth,
    boxShadow: "none",
    transform: "none",
  };
};
