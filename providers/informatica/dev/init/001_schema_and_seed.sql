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

-- ============================================================
-- Lineage demo schema
-- Source tables: customers, products, orders
-- Target tables: order_summary, customer_ltv
-- ============================================================

SET TIME ZONE 'UTC';

-- ------------------------------------------------------------
-- Source tables
-- ------------------------------------------------------------

CREATE TABLE customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    first_name    TEXT        NOT NULL,
    last_name     TEXT        NOT NULL,
    email         TEXT        NOT NULL UNIQUE,
    country       TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    product_id    BIGSERIAL PRIMARY KEY,
    sku           TEXT        NOT NULL UNIQUE,
    product_name  TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL
);

CREATE TABLE orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES customers(customer_id),
    product_id    BIGINT      NOT NULL REFERENCES products(product_id),
    quantity      INT         NOT NULL DEFAULT 1,
    unit_price    NUMERIC(12,2) NOT NULL,
    order_date    DATE        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'completed'
);

CREATE TABLE order_items (
    item_id       BIGSERIAL PRIMARY KEY,
    order_id      BIGINT      NOT NULL REFERENCES orders(order_id),
    product_id    BIGINT      NOT NULL REFERENCES products(product_id),
    quantity      INT         NOT NULL,
    line_total    NUMERIC(12,2) NOT NULL
);

-- ------------------------------------------------------------
-- Target / aggregation tables (populated by DAG tasks)
-- ------------------------------------------------------------

CREATE TABLE order_summary (
    order_id         BIGINT      PRIMARY KEY,
    customer_email   TEXT        NOT NULL,
    product_name     TEXT        NOT NULL,
    category         TEXT        NOT NULL,
    quantity         INT         NOT NULL,
    total_amount     NUMERIC(12,2) NOT NULL,
    order_date       DATE        NOT NULL,
    country          TEXT        NOT NULL,
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE customer_ltv (
    customer_id      BIGINT      PRIMARY KEY,
    email            TEXT        NOT NULL,
    full_name        TEXT        NOT NULL,
    country          TEXT        NOT NULL,
    total_orders     BIGINT      NOT NULL DEFAULT 0,
    total_spent      NUMERIC(14,2) NOT NULL DEFAULT 0,
    avg_order_value  NUMERIC(14,2),
    first_order_date DATE,
    last_order_date  DATE,
    calculated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Seed data
-- ------------------------------------------------------------

INSERT INTO customers (first_name, last_name, email, country) VALUES
    ('Alice',   'Johnson',  'alice@example.com',   'US'),
    ('Bob',     'Smith',    'bob@example.com',      'UK'),
    ('Carlos',  'Garcia',   'carlos@example.com',   'ES'),
    ('Diana',   'Lee',      'diana@example.com',    'DE'),
    ('Eve',     'Martinez', 'eve@example.com',      'FR'),
    ('Frank',   'Brown',    'frank@example.com',    'US'),
    ('Grace',   'Wilson',   'grace@example.com',    'CA'),
    ('Hiro',    'Tanaka',   'hiro@example.com',     'JP');

INSERT INTO products (sku, product_name, category, unit_price) VALUES
    ('LAPTOP-001',  'Pro Laptop 15"',     'Electronics',  1299.99),
    ('PHONE-001',   'Smart Phone X',      'Electronics',   799.99),
    ('DESK-001',    'Standing Desk',      'Furniture',     499.99),
    ('CHAIR-001',   'Ergonomic Chair',    'Furniture',     349.99),
    ('MONITOR-001', 'UltraWide 34"',      'Electronics',   699.99),
    ('HEADSET-001', 'Noise-Cancel HS',    'Electronics',   249.99),
    ('KEYBOARD-001','Mech Keyboard',      'Electronics',   149.99),
    ('MOUSE-001',   'Wireless Mouse',     'Electronics',    69.99);

INSERT INTO orders (customer_id, product_id, quantity, unit_price, order_date) VALUES
    (1, 1, 1, 1299.99, '2024-01-05'),
    (1, 6, 1,  249.99, '2024-01-10'),
    (2, 2, 1,  799.99, '2024-01-12'),
    (2, 7, 2,  149.99, '2024-02-01'),
    (3, 3, 1,  499.99, '2024-02-14'),
    (3, 4, 1,  349.99, '2024-02-14'),
    (4, 5, 2,  699.99, '2024-03-03'),
    (5, 1, 1, 1299.99, '2024-03-15'),
    (5, 8, 1,   69.99, '2024-03-15'),
    (6, 2, 1,  799.99, '2024-04-01'),
    (6, 6, 1,  249.99, '2024-04-05'),
    (7, 3, 2,  499.99, '2024-04-20'),
    (8, 5, 1,  699.99, '2024-05-01'),
    (8, 7, 1,  149.99, '2024-05-10'),
    (1, 8, 3,   69.99, '2024-05-15');

INSERT INTO order_items (order_id, product_id, quantity, line_total) VALUES
    (1,  1, 1, 1299.99),
    (2,  6, 1,  249.99),
    (3,  2, 1,  799.99),
    (4,  7, 2,  299.98),
    (5,  3, 1,  499.99),
    (6,  4, 1,  349.99),
    (7,  5, 2, 1399.98),
    (8,  1, 1, 1299.99),
    (9,  8, 1,   69.99),
    (10, 2, 1,  799.99),
    (11, 6, 1,  249.99),
    (12, 3, 2,  999.98),
    (13, 5, 1,  699.99),
    (14, 7, 1,  149.99),
    (15, 8, 3,  209.97);
