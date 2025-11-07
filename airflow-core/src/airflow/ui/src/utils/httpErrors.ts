/*
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

/**
 * Checks if an error object has a specific HTTP status code
 * Handles various error formats including direct status, response.status, and nested error structures
 * 
 * @param error - The error object to check
 * @param status - The HTTP status code to match
 * @returns true if the error has the specified status code, false otherwise
 */
export const isHttpError = (error: unknown, status: number): boolean => {
  if (!error || typeof error !== 'object') return false;
  
  // Check for direct status property
  if ('status' in error && error.status === status) return true;
  
  // Check for response.status (common in fetch errors)
  if ('response' in error && 
      error.response && 
      typeof error.response === 'object' && 
      'status' in error.response && 
      error.response.status === status) return true;
  
  // Check for nested error structures
  if ('error' in error && 
      error.error && 
      typeof error.error === 'object' && 
      'status' in error.error && 
      error.error.status === status) return true;
  
  return false;
};