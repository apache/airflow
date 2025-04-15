# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This module maintains custom SQL functions for databases that do not natively support
# certain functionalities. These functions are intended for performance optimization
# and compatibility, particularly for large tables like the task_instance table.
# Currently, it includes functions like UUID v7 generation for databases such as
# Postgres and MySQL, and can be extended in the future for other functionalities.

from __future__ import annotations

POSTGRES_UUID7_FN = """
DO $$
DECLARE
    pgcrypto_installed BOOLEAN;
BEGIN
    -- Check if pgcrypto is already installed
    pgcrypto_installed := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto');

    -- Attempt to create pgcrypto if it is not installed
    IF NOT pgcrypto_installed THEN
        BEGIN
            CREATE EXTENSION pgcrypto;
            pgcrypto_installed := TRUE;
            RAISE NOTICE 'pgcrypto extension successfully created.';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'pgcrypto extension could not be installed due to insufficient privileges; using fallback';
                pgcrypto_installed := FALSE;
            WHEN OTHERS THEN
                RAISE NOTICE 'An unexpected error occurred while attempting to install pgcrypto; using fallback';
                pgcrypto_installed := FALSE;
        END;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION uuid_generate_v7(p_timestamp timestamp with time zone)
RETURNS uuid
LANGUAGE plpgsql
PARALLEL SAFE
AS $$
DECLARE
    unix_time_ms CONSTANT bytea NOT NULL DEFAULT substring(int8send((extract(epoch FROM p_timestamp) * 1000)::bigint) from 3);
    buffer bytea;
    pgcrypto_installed BOOLEAN := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto');
BEGIN
    -- Use pgcrypto if available, otherwise use the fallback
    -- fallback from https://brandur.org/fragments/secure-bytes-without-pgcrypto
    IF pgcrypto_installed THEN
        buffer := unix_time_ms || gen_random_bytes(10);
    ELSE
        buffer := unix_time_ms || substring(uuid_send(gen_random_uuid()) FROM 1 FOR 5) ||
                  substring(uuid_send(gen_random_uuid()) FROM 12 FOR 5);
    END IF;

    -- Set UUID version and variant bits
    buffer := set_byte(buffer, 6, (b'0111' || get_byte(buffer, 6)::bit(4))::bit(8)::int);
    buffer := set_byte(buffer, 8, (b'10'   || get_byte(buffer, 8)::bit(6))::bit(8)::int);
    RETURN encode(buffer, 'hex')::uuid;
END
$$;
"""

POSTGRES_UUID7_FN_DROP = """
DROP FUNCTION IF EXISTS uuid_generate_v7(timestamp with time zone);
"""

# MySQL-specific UUID v7 function
MYSQL_UUID7_FN = """
DROP FUNCTION IF EXISTS uuid_generate_v7;
CREATE FUNCTION uuid_generate_v7(p_timestamp DATETIME(3))
RETURNS CHAR(36)
DETERMINISTIC
BEGIN
    DECLARE unix_time_ms BIGINT;
    DECLARE time_hex CHAR(12);
    DECLARE rand_hex CHAR(24);
    DECLARE uuid CHAR(36);

    -- Convert the passed timestamp to milliseconds since epoch
    SET unix_time_ms = UNIX_TIMESTAMP(p_timestamp) * 1000;
    SET time_hex = LPAD(HEX(unix_time_ms), 12, '0');
    SET rand_hex = CONCAT(
        LPAD(HEX(FLOOR(RAND() * POW(2,32))), 8, '0'),
        LPAD(HEX(FLOOR(RAND() * POW(2,32))), 8, '0')
    );
    SET rand_hex = CONCAT(SUBSTRING(rand_hex, 1, 4), '7', SUBSTRING(rand_hex, 6));
    SET rand_hex = CONCAT(SUBSTRING(rand_hex, 1, 12), '8', SUBSTRING(rand_hex, 14));

    SET uuid = LOWER(CONCAT(
        SUBSTRING(time_hex, 1, 8), '-',
        SUBSTRING(time_hex, 9, 4), '-',
        SUBSTRING(rand_hex, 1, 4), '-',
        SUBSTRING(rand_hex, 5, 4), '-',
        SUBSTRING(rand_hex, 9)
    ));

    RETURN uuid;
END;
"""

MYSQL_UUID7_FN_DROP = """
DROP FUNCTION IF EXISTS uuid_generate_v7;
"""
