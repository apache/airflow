/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

DATABASE {{params.DATABASE_NAME}};
CREATE SET TABLE {{params.TABLE_NAME}} (
          emp_id INT,
          emp_name VARCHAR(100),
          dept VARCHAR(50)
        ) PRIMARY INDEX (emp_id);
INSERT INTO {{params.TABLE_NAME}} VALUES (1, 'John Doe', 'IT');
INSERT INTO {{params.TABLE_NAME}} VALUES (2, 'Jane Smith', 'HR');
SELECT * FROM {{params.TABLE_NAME}};
DROP TABLE {{params.TABLE_NAME}};
