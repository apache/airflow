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

const providersData = require('./providers.json');

module.exports = function() {
  const providers = providersData.providers;

  // Total providers count
  const totalProviders = providers.length;

  // Count by lifecycle stage (AIP-95)
  const lifecycleCounts = providers.reduce((acc, p) => {
    const lc = p.lifecycle || 'production';
    const display = lc === 'incubation' ? 'incubation' : (lc === 'deprecated' ? 'deprecated' : 'stable');
    acc[display] = (acc[display] || 0) + 1;
    return acc;
  }, {});

  const lifecycleStats = Object.entries(lifecycleCounts).map(([stage, count]) => ({
    stage,
    count,
    percentage: (count / totalProviders * 100).toFixed(1)
  }));

  // Aggregate module counts across all providers
  const aggregateModuleCounts = providers.reduce((acc, p) => {
    if (p.module_counts) {
      Object.entries(p.module_counts).forEach(([type, count]) => {
        acc[type] = (acc[type] || 0) + count;
      });
    }
    return acc;
  }, {});

  const totalModules = Object.values(aggregateModuleCounts).reduce((a, b) => a + b, 0);

  // Module type metadata
  const moduleTypeInfo = {
    operator: {
      label: 'Operators',
      icon: 'O',
      colorClass: 'operator'
    },
    hook: {
      label: 'Hooks',
      icon: 'H',
      colorClass: 'hook'
    },
    sensor: {
      label: 'Sensors',
      icon: 'S',
      colorClass: 'sensor'
    },
    trigger: {
      label: 'Triggers',
      icon: 'T',
      colorClass: 'trigger'
    },
    transfer: {
      label: 'Transfers',
      icon: 'X',
      colorClass: 'transfer'
    },
    bundle: {
      label: 'Bundles',
      icon: 'B',
      colorClass: 'bundle'
    },
    notifier: {
      label: 'Notifiers',
      icon: 'N',
      colorClass: 'notifier'
    },
    secret: {
      label: 'Secrets Backend',
      icon: 'K',
      colorClass: 'secret'
    },
    logging: {
      label: 'Log Handler',
      icon: 'L',
      colorClass: 'logging'
    },
    executor: {
      label: 'Executors',
      icon: 'E',
      colorClass: 'executor'
    },
    decorator: {
      label: 'Decorators',
      icon: '@',
      colorClass: 'decorator'
    }
  };

  // Module type display data with counts and percentages
  const moduleTypeStats = Object.entries(moduleTypeInfo).map(([type, info]) => {
    const count = aggregateModuleCounts[type] || 0;
    const percentage = totalModules > 0 ? ((count / totalModules) * 100).toFixed(1) : 0;
    return {
      type,
      ...info,
      count,
      percentage
    };
  });

  // Total monthly downloads across all providers
  const totalDownloads = providers.reduce((sum, p) => {
    return sum + ((p.pypi_downloads && p.pypi_downloads.monthly) || 0);
  }, 0);

  // Total unique connection types across all providers
  const allConnectionTypes = new Set();
  providers.forEach(p => {
    if (p.connection_types) {
      p.connection_types.forEach(ct => {
        allConnectionTypes.add(ct.conn_type || ct.connection_type);
      });
    }
  });
  const totalConnectionTypes = allConnectionTypes.size;

  // Enriched provider list with totals
  const enrichedProviders = [...providers].map(p => ({
    ...p,
    totalModules: p.module_counts
      ? Object.values(p.module_counts).reduce((a, b) => a + b, 0)
      : 0,
    monthlyDownloads: (p.pypi_downloads && p.pypi_downloads.monthly) || 0,
    weeklyDownloads: (p.pypi_downloads && p.pypi_downloads.weekly) || 0
  }));

  // Exclude zero-module providers from rankings
  const withModules = enrichedProviders.filter(p => p.totalModules > 0);

  // Top 10 providers by module count
  const topProviders = [...withModules]
    .sort((a, b) => b.totalModules - a.totalModules)
    .slice(0, 10);

  // Top 10 providers by monthly downloads
  const topByDownloads = [...withModules]
    .sort((a, b) => b.monthlyDownloads - a.monthlyDownloads)
    .slice(0, 10);

  return {
    totalProviders,
    totalModules,
    totalDownloads,
    totalConnectionTypes,
    lifecycleCounts,
    lifecycleStats,
    moduleTypeStats,
    topProviders,
    topByDownloads
  };
};
