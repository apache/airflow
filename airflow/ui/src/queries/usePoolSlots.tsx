import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip, PieChart, Pie, Cell } from 'recharts';
import { Box, HStack, Text } from '@chakra-ui/react';
import { usePoolServiceGetPools } from 'openapi/queries';


export const Poolslots = () => {
    const { data, isLoading } = usePoolServiceGetPools();
    
    if (isLoading) return <div>Loading...</div>;
    if (!data) return <div>No data</div>;

    const stats = {
        running: data.pools.reduce((acc, pool) => acc + pool.occupied_slots, 0),
        queued: data.pools.reduce((acc, pool) => acc + pool.queued_slots, 0),
        deferred: data.pools.reduce((acc, pool) => acc + pool.deferred_slots, 0),
        scheduled: data.pools.reduce((acc, pool) => acc + pool.scheduled_slots, 0),
        open: data.pools.reduce((acc, pool) => acc + pool.open_slots, 0)
    };

    const transformedData = [{
        running: stats.running,
        queued: stats.queued,
        deferred: stats.deferred,
        scheduled: stats.scheduled,
        open: stats.open
    }];

    const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];

    return (
        <PieChart width={800} height={400}>
        <Pie
          data={transformedData}
          cx={120}
          cy={200}
          innerRadius={60}
          outerRadius={80}
          fill="#8884d8"
          paddingAngle={5}
          dataKey="value"
        >
          {transformedData.map((entry, index) => (
            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
          ))}
        </Pie>
      </PieChart>
    );
};