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

import { useToast } from '@chakra-ui/react';

export const getErrorDescription = (error, fallbackMessage) => {
  if (error && error.response && error.response.data) {
    return error.response.data;
  }
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  return fallbackMessage || 'Something went wrong.';
};

const getErrorTitle = (error) => (error.message || 'Error');

const useErrorToast = () => {
  const toast = useToast();
  // Add an error prop and handle it as a description
  return ({ error, ...rest }) => {
    toast({
      ...rest,
      status: 'error',
      title: getErrorTitle(error),
      description: getErrorDescription(error).slice(0, 500),
    });
  };
};

export default useErrorToast;
