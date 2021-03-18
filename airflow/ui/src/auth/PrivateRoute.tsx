import React, {
  FC,
} from 'react';
import { Route, RouteProps } from 'react-router-dom';

import Login from 'views/Login';
import { useAuthContext } from './context';

const PrivateRoute: FC<RouteProps> = (props) => {
  const { hasValidAuthToken } = useAuthContext();
  return hasValidAuthToken ? <Route {...props} /> : <Login />;
};

export default PrivateRoute;
