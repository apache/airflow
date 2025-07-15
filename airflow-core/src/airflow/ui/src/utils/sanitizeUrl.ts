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

type UrlType = "bundle" | "iframe";

/**
 * Sanitizes URLs for safe use in iframe src attributes or dynamic imports
 * Prevents XSS by validating protocol and removing dangerous content
 */
export const sanitizeUrl = (url: string, type: UrlType): string => {
  try {
    // Parse the URL to validate its structure
    const urlObj = new URL(url);

    // Only allow http, https, and relative URLs
    if (urlObj.protocol !== "http:" && urlObj.protocol !== "https:" && urlObj.protocol !== "") {
      throw new Error("Invalid protocol");
    }

    // Additional validation for bundle URLs
    if (type === "bundle") {
      const pathname = urlObj.pathname.toLowerCase();

      if (!pathname.endsWith(".js") && !pathname.endsWith(".mjs") && !pathname.includes("/")) {
        throw new Error("Invalid bundle URL");
      }
    }

    // Remove any potentially dangerous query parameters
    const cleanUrl = new URL(urlObj.toString());
    const dangerousParams = ["javascript:", "data:"];

    // Filter out dangerous parameters
    for (const [key, value] of cleanUrl.searchParams.entries()) {
      if (
        dangerousParams.some(
          (dangerous) => key.toLowerCase().includes(dangerous) || value.toLowerCase().includes(dangerous),
        )
      ) {
        cleanUrl.searchParams.delete(key);
      }
    }

    return cleanUrl.toString();
  } catch {
    // If URL parsing fails, check if it's a safe relative URL
    if (url.startsWith("/") || url.startsWith("./") || url.startsWith("../")) {
      return url;
    }

    // Default to a safe fallback based on type
    if (type === "bundle") {
      throw new Error("Invalid bundle URL");
    }

    return "/";
  }
};

/**
 * Convenience function for iframe URL sanitization
 */
export const sanitizeIframeUrl = (url: string): string => sanitizeUrl(url, "iframe");

/**
 * Convenience function for bundle URL sanitization
 */
export const sanitizeBundleUrl = (url: string): string => sanitizeUrl(url, "bundle");
