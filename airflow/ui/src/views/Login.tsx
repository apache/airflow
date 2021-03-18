import React, { useState, FormEvent } from 'react';
import {
  Box,
  Button,
  FormLabel,
  FormControl,
  Icon,
  Input,
  InputGroup,
  InputLeftElement,
  Spinner,
  Alert,
  AlertIcon,
} from '@chakra-ui/react';
import { MdLock, MdPerson } from 'react-icons/md';

import { useAuthContext } from 'auth/context';

const Login: React.FC = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const { login, error, loading } = useAuthContext();

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    login(username, password);
  };

  return (
    <Box display="flex" alignItems="center" justifyContent="center" height="80vh">
      <Box as="form" width="100%" maxWidth="400px" mx="auto" onSubmit={onSubmit}>
        <FormControl>
          <FormLabel htmlFor="username">Username</FormLabel>
          <InputGroup>
            <InputLeftElement>
              <Icon as={MdPerson} color="gray.300" />
            </InputLeftElement>
            <Input
              autoFocus
              autoCapitalize="none"
              name="username"
              placeholder="Username"
              data-testid="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              isRequired
            />
          </InputGroup>
        </FormControl>
        <FormControl mt={4}>
          <FormLabel htmlFor="password">Password</FormLabel>
          <InputGroup>
            <InputLeftElement>
              <Icon as={MdLock} color="gray.300" />
            </InputLeftElement>
            <Input
              type="password"
              name="password"
              placeholder="Password"
              data-testid="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              isRequired
            />
          </InputGroup>
        </FormControl>
        <Button
          width="100%"
          mt={4}
          type="submit"
          disabled={!username || !password}
          data-testid="submit"
        >
          {loading ? <Spinner size="md" speed="0.85s" /> : 'Log in'}
        </Button>
        {error && (
          <Alert status="error" my="4" key={error.message}>
            <AlertIcon />
            {error.message}
          </Alert>
        )}
      </Box>
    </Box>
  );
};

export default Login;
