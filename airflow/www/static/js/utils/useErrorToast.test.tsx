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

/* global describe, test, expect */

import { getErrorDescription } from './useErrorToast';

describe('Test getErrorDescription()', () => {
  test('Returns expected results', () => {
    let description;

    // is response.data is defined
    description = getErrorDescription({ response: { data: 'uh oh' } });
    expect(description).toBe('uh oh');

    // if it is not, use default message
    description = getErrorDescription({ response: { data: '' } });
    expect(description).toBe('Something went wrong.');

    // if error object, return the message
    description = getErrorDescription(new Error('no no'));
    expect(description).toBe('no no');

    // if string, return the string
    description = getErrorDescription('error!');
    expect(description).toBe('error!');

    // if it's undefined, use a fallback
    description = getErrorDescription(null, 'fallback');
    expect(description).toBe('fallback');

    // use default if nothing is defined
    description = getErrorDescription();
    expect(description).toBe('Something went wrong.');
  });
});
