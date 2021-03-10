import { hot } from 'react-hot-loader';
import React from 'react';
import { Center, Heading } from '@chakra-ui/react';

const App = () => (
  <Center height="100vh">
    <Heading>Apache Airflow new UI</Heading>
  </Center>
);

export default hot(module)(App);
