import React, {
  useState, useEffect, useCallback, ReactNode, ReactElement,
} from 'react';
import axios from 'axios';
import { useQueryClient } from 'react-query';

import {
  checkExpire, clearAuth, get, set,
} from 'utils/localStorage';
import { AuthContext } from './context';

type Props = {
  children: ReactNode;
};

const AuthProvider = ({ children }: Props): ReactElement => {
  const [hasValidAuthToken, setHasValidAuthToken] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const queryClient = useQueryClient();

  const clearData = useCallback(() => {
    setHasValidAuthToken(false);
    clearAuth();
    queryClient.clear();
    axios.defaults.headers.common.Authorization = null;
  }, [queryClient]);

  const logout = () => clearData();

  // intercept responses and logout on unauthorized error
  axios.interceptors.response.use(
    (res) => res,
    (err) => {
      if (err && err.response && err.response.status === 401) {
        logout();
      }
      return Promise.reject(err);
    },
  );

  useEffect(() => {
    const token = get('token');
    const isExpired = checkExpire('token');
    if (token && !isExpired) {
      axios.defaults.headers.common.Authorization = token;
      setHasValidAuthToken(true);
    } else if (token) {
      clearData();
      setError(new Error('Token invalid, please reauthenticate.'));
    } else {
      setHasValidAuthToken(false);
    }
    setLoading(false);
  }, [clearData]);

  // Login with basic auth.
  // There is no actual auth endpoint yet, so we check against a generic endpoint
  const login = async (username: string, password: string) => {
    setLoading(true);
    setError(null);
    try {
      const authorization = `Basic ${btoa(`${username}:${password}`)}`;
      await axios.get(`${process.env.API_URL}config`, {
        headers: {
          Authorization: authorization,
        },
      });
      set('token', authorization);
      axios.defaults.headers.common.Authorization = authorization;
      setLoading(false);
      setHasValidAuthToken(true);
    } catch (e) {
      setLoading(false);
      setError(e);
    }
  };

  return (
    <AuthContext.Provider
      value={{
        hasValidAuthToken,
        logout,
        login,
        loading,
        error,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};

export default AuthProvider;
