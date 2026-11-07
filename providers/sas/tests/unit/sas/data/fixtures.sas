/*
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

/*
 * The SAS program that wrote the .sas7bdat files next to it (SAS 9.4,
 * session encoding latin9 / iso-8859-15). Dummy data only. To regenerate
 * them, point the libname at a directory you can write to, run it, and
 * copy the data sets here.
 */
libname fx "/tmp/sas_fixtures";

/* Every column kind and format the hook types, with missing values,
   special missing values, padding, quotes and Latin-9 characters. */
data fx.all_types(compress=no);
    length id 8 small_int 3 amount 8
           c_short $8 c_long $300 c_padded $10 c_quotes $40 c_latin $20;
    format d_date date9. d_ymd yymmdd10.
           dt_sec datetime20. dt_us datetime26.6 dt_iso e8601dt19.
           t_time time8. amount comma12.2;

    id = 1; small_int = 1; amount = 1234.5;
    c_short = 'abc'; c_long = repeat('x', 299); c_padded = '  pad';
    c_quotes = 'single '' and "double"'; c_latin = 'éàüß€';
    d_date = '01JAN1960'd; d_ymd = '31DEC2099'd;
    dt_sec = '01JAN1960:00:00:00'dt; dt_us = '15MAR2024:13:45:30.123456'dt;
    dt_iso = '29FEB2024:08:00:00'dt; t_time = '23:59:59't;
    output;

    id = 2; small_int = .; amount = .A;
    c_short = ''; c_long = ''; c_padded = ''; c_quotes = ''; c_latin = '';
    d_date = .; d_ymd = .Z; dt_sec = .; dt_us = ._; dt_iso = .; t_time = .;
    output;

    id = 3; small_int = -4096; amount = -0.01;
    c_short = 'ABCDEFGH'; c_long = 'short'; c_padded = 'x'; c_quotes = ';--';
    c_latin = 'Ñandú œuvre';
    d_date = '01JAN1900'd; d_ymd = '29FEB2000'd;
    dt_sec = '31DEC1959:23:59:59'dt; dt_us = '01JAN2000:00:00:00.000001'dt;
    dt_iso = '01JAN1970:00:00:00'dt; t_time = '00:00:00't;
    output;

    id = 4; small_int = 8191; amount = 9007199254740992;
    c_short = '12345678'; c_long = 'x'; c_padded = 'trail   '; c_quotes = '%nrstr(&x)';
    c_latin = 'Ÿ';
    d_date = '28FEB2026'd; d_ymd = '01JAN1582'd;
    dt_sec = '03NOV2026:12:00:00'dt; dt_us = .; dt_iso = .; t_time = '12:34:56't;
    output;
run;

/* The same table under each COMPRESS= value the hook can ask for. */
data fx.all_types_char(compress=char);   set fx.all_types; run;
data fx.all_types_binary(compress=binary); set fx.all_types; run;

/* A larger table for chunked reads, prefetch bounds and LIMIT/OFFSET paging. */
data fx.many_rows(compress=yes);
    length id 8 amount 8 label $12;
    format dt datetime20.;
    do id = 1 to 10000;
        amount = id * 1.5;
        label = cats('row', id);
        dt = '01JAN2020:00:00:00'dt + id * 60;
        output;
    end;
run;

/* Optional, only if you can start a session with -encoding utf8:
   run this block in that session to get a UTF-8 encoded fixture.

data fx.all_types_utf8(encoding='utf-8');
    set fx.all_types;
run;
*/

proc contents data=fx._all_ short; run;
