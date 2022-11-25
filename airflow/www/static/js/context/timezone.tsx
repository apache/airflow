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

/* global moment, document */

import React, {
  useContext, useEffect, useState, useMemo, PropsWithChildren,
} from 'react';

import { TimezoneEvent } from '../datetime_utils';

const TimezoneContext = React.createContext({ timezone: 'UTC' });

export const TimezoneProvider = ({ children }: PropsWithChildren) => {
  // @ts-ignores: defaultZone not recognize in moment.
  const [timezone, setTimezone] = useState((moment.defaultZone && moment.defaultZone.name) || 'UTC');

  const handleChange = (e: CustomEvent<string>) => {
    if (e.detail && e.detail !== timezone) setTimezone(e.detail);
  };

  useEffect(() => {
    document.addEventListener(TimezoneEvent, handleChange as EventListener);
    return () => {
      document.removeEventListener(TimezoneEvent, handleChange as EventListener);
    };
  });

  const value = useMemo(() => ({ timezone }), [timezone]);

  return (
    <TimezoneContext.Provider value={value}>
      {children}
    </TimezoneContext.Provider>
  );
};

export const useTimezone = () => useContext(TimezoneContext);
