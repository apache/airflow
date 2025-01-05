import React from 'react';
import { usePools } from '../../../queries/usePoolSlots';
import { HStack, Box, Text } from '@chakra-ui/react';

interface Pool {
    running: number;
    queued: number;
    deferred: number;
    scheduled: number;
    open: number;
}

export const PoolSlotStats = () => {
    const { data } = usePools();

    if (!data?.pools) return null;

    const calculateTotal = (pool: Pool) => {
        return pool.running + pool.queued + pool.deferred + pool.scheduled + pool.open;
    };

    return (
        <Box width="full" maxWidth="3xl">
            {data.pools.map((stat: Pool, index: number) => {
                const total = calculateTotal(stat);
                return (
                    <Box key={index} mb={4}>
                        <HStack 
                            height="32px" 
                            width="full" 
                            borderRadius="full" 
                            overflow="hidden"
                        >
                            <Box
                                width={`${(stat.running / total) * 100}%`}
                                bg="blue.500"
                                height="full"
                                display="flex"
                                alignItems="center"
                                justifyContent="center"
                            >
                                <Text fontSize="xs" color="white">
                                    {stat.running > 0 && stat.running}
                                </Text>
                            </Box>
                            <Box
                                width={`${(stat.queued / total) * 100}%`}
                                bg="yellow.500"
                                height="full"
                                display="flex"
                                alignItems="center"
                                justifyContent="center"
                            >
                                <Text fontSize="xs" color="white">
                                    {stat.queued > 0 && stat.queued}
                                </Text>
                            </Box>
                            <Box
                                width={`${(stat.deferred / total) * 100}%`}
                                bg="purple.500"
                                height="full"
                                display="flex"
                                alignItems="center"
                                justifyContent="center"
                            >
                                <Text fontSize="xs" color="white">
                                    {stat.deferred > 0 && stat.deferred}
                                </Text>
                            </Box>
                            <Box
                                width={`${(stat.scheduled / total) * 100}%`}
                                bg="green.500"
                                height="full"
                                display="flex"
                                alignItems="center"
                                justifyContent="center"
                            >
                                <Text fontSize="xs" color="white">
                                    {stat.scheduled > 0 && stat.scheduled}
                                </Text>
                            </Box>
                            <Box
                                width={`${(stat.open / total) * 100}%`}
                                bg="gray.500"
                                height="full"
                                display="flex"
                                alignItems="center"
                                justifyContent="center"
                            >
                                <Text fontSize="xs" color="white">
                                    {stat.open > 0 && stat.open}
                                </Text>
                            </Box>
                        </HStack>

           
                        <HStack mt={2} >
                            <HStack>
                                <Box w="12px" h="12px" borderRadius="full" bg="blue.500" />
                                <Text fontSize="sm">Running</Text>
                            </HStack>
                            <HStack>
                                <Box w="12px" h="12px" borderRadius="full" bg="yellow.500" />
                                <Text fontSize="sm">Queued</Text>
                            </HStack>
                            <HStack>
                                <Box w="12px" h="12px" borderRadius="full" bg="purple.500" />
                                <Text fontSize="sm">Deferred</Text>
                            </HStack>
                            <HStack>
                                <Box w="12px" h="12px" borderRadius="full" bg="green.500" />
                                <Text fontSize="sm">Scheduled</Text>
                            </HStack>
                            <HStack>
                                <Box w="12px" h="12px" borderRadius="full" bg="gray.500" />
                                <Text fontSize="sm">Open</Text>
                            </HStack>
                        </HStack>
                    </Box>
                );
            })}
        </Box>
    );
};