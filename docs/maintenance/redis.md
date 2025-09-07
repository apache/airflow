

\# Redis Maintenance



\## Introduction



When running Airflow with the \*\*CeleryExecutor\*\* or \*\*CeleryKubernetesExecutor\*\*, Redis is often used as the message broker.

Redis stores task-related data, queue information, and other temporary state.



Over time, Redis can grow very large if keys are not cleaned up.

This can lead to performance problems or even production outages if the instance runs out of memory.



This guide describes how to monitor, clean, and maintain your Redis instance in an Airflow setup.



---



\## Why Maintenance Is Needed



Redis does not automatically remove all keys created by Celery or Kombu (the library Airflow uses for queuing).

If keys are left without a \*\*Time-To-Live (TTL)\*\*, they can accumulate indefinitely.



\### Common symptoms



\* Redis memory usage grows continuously.

\* Instance reaches the maximum memory limit and stops accepting writes.

\* Airflow tasks start failing due to Redis errors.



---



\## Recommended Cleanup Strategy



\### 1. Use TTLs on Redis keys



If you discover keys without TTLs, you can enforce expiry manually.

The example below uses a Lua script to set TTLs for all keys except critical ones.



```bash

\#!/bin/bash



\# Usage: ./cleanup.sh -h <host> -p <port>



HOST=$1

PORT=$2



\# Inline Lua script to set TTL on keys

lua\_script='

local keys = redis.call("KEYS", "\*")

for \_, key in ipairs(keys) do

&nbsp;   if string.match(key, "^unacked") or string.match(key, "^\_kombu%.binding") then

&nbsp;       -- Skip critical keys

&nbsp;   else

&nbsp;       if redis.call("TTL", key) == -1 then

&nbsp;           redis.call("EXPIRE", key, 54000)  -- 15 days

&nbsp;       end

&nbsp;   end

end

'



redis6-cli -h "$HOST" -p "$PORT" --eval <(echo "$lua\_script")

```



This script:



\* Skips keys that should not be expired (`unacked\*`, `\_kombu.binding\*`).

\* Adds a \*\*15-day TTL\*\* to all other keys that currently have no TTL.



Run it as a cron job (e.g., daily) to prevent Redis from filling up.



---



\### 2. Monitor Redis usage



Regularly check:



```bash

redis-cli info memory

```



Key metrics to watch:



\* `used\_memory\_human` → current memory usage.

\* `maxmemory` → configured memory limit.

\* `evicted\_keys` → if greater than 0, Redis is dropping keys due to memory pressure.



---



\### 3. Configure eviction policy



Set `maxmemory-policy` to an appropriate value (e.g., `allkeys-lru`) to ensure Redis can evict old keys when full.

This prevents outages, but should not replace proper cleanup.



---



\## Notes for Airflow 2.x vs 3.x



\* In \*\*Airflow 2.x\*\*, unused keys from Celery were a common cause of Redis growth.

\* In \*\*Airflow 3.x\*\*, some cleanup improvements may exist, but you should still monitor Redis closely.

\* Always test cleanup scripts before applying them in production, as internal prefixes may change between versions.



---



\## Conclusion



Redis is a critical component for Airflow when using Celery.

Without maintenance, Redis can grow indefinitely and cause outages.



To keep your system stable:



1\. Monitor Redis memory usage.

2\. Enforce TTLs on keys.

3\. Exclude important prefixes (`unacked\*`, `\_kombu.binding\*`).

4\. Configure eviction policies as a safety net.



Following these steps will help ensure your Redis instance stays healthy and your Airflow tasks run smoothly.



---



✅ That’s your \*\*final file content\*\*.



Would you like me to now walk you through the \*\*exact terminal commands\*\* to save, commit, and push this file into your Git branch so you’re ready to open a PR?

ect 👍 Here’s the \*\*plain-text final doc\*\* — no extra formatting, no “markdown” labels, nothing to confuse you.



Just copy-paste \*\*everything below\*\* into your new file `docs/maintenance/redis.md`:



---



\# Redis Maintenance



\## Introduction



When running Airflow with the \*\*CeleryExecutor\*\* or \*\*CeleryKubernetesExecutor\*\*, Redis is often used as the message broker.

Redis stores task-related data, queue information, and other temporary state.



Over time, Redis can grow very large if keys are not cleaned up.

This can lead to performance problems or even production outages if the instance runs out of memory.



This guide describes how to monitor, clean, and maintain your Redis instance in an Airflow setup.



---



\## Why Maintenance Is Needed



Redis does not automatically remove all keys created by Celery or Kombu (the library Airflow uses for queuing).

If keys are left without a \*\*Time-To-Live (TTL)\*\*, they can accumulate indefinitely.



\### Common symptoms



\* Redis memory usage grows continuously.

\* Instance reaches the maximum memory limit and stops accepting writes.

\* Airflow tasks start failing due to Redis errors.



---



\## Recommended Cleanup Strategy



\### 1. Use TTLs on Redis keys



If you discover keys without TTLs, you can enforce expiry manually.

The example below uses a Lua script to set TTLs for all keys except critical ones.



```bash

\#!/bin/bash



\# Usage: ./cleanup.sh -h <host> -p <port>



HOST=$1

PORT=$2



\# Inline Lua script to set TTL on keys

lua\_script='

local keys = redis.call("KEYS", "\*")

for \_, key in ipairs(keys) do

&nbsp;   if string.match(key, "^unacked") or string.match(key, "^\_kombu%.binding") then

&nbsp;       -- Skip critical keys

&nbsp;   else

&nbsp;       if redis.call("TTL", key) == -1 then

&nbsp;           redis.call("EXPIRE", key, 54000)  -- 15 days

&nbsp;       end

&nbsp;   end

end

'



redis6-cli -h "$HOST" -p "$PORT" --eval <(echo "$lua\_script")

```



This script:



\* Skips keys that should not be expired (`unacked\*`, `\_kombu.binding\*`).

\* Adds a \*\*15-day TTL\*\* to all other keys that currently have no TTL.



Run it as a cron job (e.g., daily) to prevent Redis from filling up.



---



\### 2. Monitor Redis usage



Regularly check:



```bash

redis-cli info memory

```



Key metrics to watch:



\* `used\_memory\_human` → current memory usage.

\* `maxmemory` → configured memory limit.

\* `evicted\_keys` → if greater than 0, Redis is dropping keys due to memory pressure.



---



\### 3. Configure eviction policy



Set `maxmemory-policy` to an appropriate value (e.g., `allkeys-lru`) to ensure Redis can evict old keys when full.

This prevents outages, but should not replace proper cleanup.



---



\## Notes for Airflow 2.x vs 3.x



\* In \*\*Airflow 2.x\*\*, unused keys from Celery were a common cause of Redis growth.

\* In \*\*Airflow 3.x\*\*, some cleanup improvements may exist, but you should still monitor Redis closely.

\* Always test cleanup scripts before applying them in production, as internal prefixes may change between versions.



---



\## Conclusion



Redis is a critical component for Airflow when using Celery.

Without maintenance, Redis can grow indefinitely and cause outages.



To keep your system stable:



1\. Monitor Redis memory usage.

2\. Enforce TTLs on keys.

3\. Exclude important prefixes (`unacked\*`, `\_kombu.binding\*`).

4\. Configure eviction policies as a safety net.



Following these steps will help ensure your Redis instance stays healthy and your Airflow tasks run smoothly.



---





