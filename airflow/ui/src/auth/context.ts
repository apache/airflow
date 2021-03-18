import {
  createContext, useContext,
} from 'react';

// todo: eventually replace hasValidAuthToken with a user object
interface AuthContextData {
  hasValidAuthToken: boolean;
  login: (username: string, password: string) => void;
  logout: () => void;
  loading: boolean;
  error: Error | null;
}

export const authContextDefaultValue: AuthContextData = {
  hasValidAuthToken: false,
  login: () => null,
  logout: () => null,
  loading: true,
  error: null,
};

export const AuthContext = createContext<AuthContextData>(authContextDefaultValue);

export const useAuthContext = () => useContext(AuthContext);
