-- Tron-themed demo schema and data for scylla-rust-mcp
-- Usage: cqlsh -f demo/encom_demo.cql

DROP KEYSPACE IF EXISTS encom;

-- Use NetworkTopologyStrategy to avoid SimpleStrategy warning in Scylla.
-- In a single-node dev setup, the default DC is often 'datacenter1'.
CREATE KEYSPACE IF NOT EXISTS encom
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

USE encom;

/*
  Entities (Tron theme):
  - customers: The Users (e.g., Kevin Flynn, Alan Bradley)
  - orders: Their actions in the Grid (lightcycle parts, identity disk upgrades)
  - order_items: Line items within an order
*/

CREATE TABLE IF NOT EXISTS customers (
  id uuid PRIMARY KEY,
  email text,
  name text
);

CREATE TABLE IF NOT EXISTS orders (
  customer_id uuid,
  order_id timeuuid,
  created_at timestamp,
  total decimal,
  status text,
  PRIMARY KEY ((customer_id), order_id)
) WITH CLUSTERING ORDER BY (order_id DESC);

CREATE TABLE IF NOT EXISTS order_items (
  order_id timeuuid,
  line_item int,
  sku text,
  qty int,
  price decimal,
  PRIMARY KEY ((order_id), line_item)
);

-- Fixed UUIDs for deterministic demos
-- Users (customers)
-- Kevin Flynn
INSERT INTO customers (id, email, name) VALUES (
  8a1f0b3c-4d2e-4a1b-9c0d-1e2f3a4b5c6d,
  'kevin.flynn@encom.example',
  'Kevin Flynn'
);

-- Alan Bradley
INSERT INTO customers (id, email, name) VALUES (
  2f1a9c34-7b8d-4f21-8a0b-1234567890ab,
  'alan.bradley@encom.example',
  'Alan Bradley'
);

-- Predefined timeuuids (v1) for orders
-- Flynn orders
INSERT INTO orders (customer_id, order_id, created_at, total, status) VALUES (
  8a1f0b3c-4d2e-4a1b-9c0d-1e2f3a4b5c6d,
  6ba7b810-9dad-11d1-80b4-00c04fd430c8,
  '2024-01-12 10:15:00+0000',
  129.95,
  'paid'
);

INSERT INTO orders (customer_id, order_id, created_at, total, status) VALUES (
  8a1f0b3c-4d2e-4a1b-9c0d-1e2f3a4b5c6d,
  6ba7b811-9dad-11d1-80b4-00c04fd430c8,
  '2024-03-03 18:42:00+0000',
  59.98,
  'shipped'
);

-- Bradley order
INSERT INTO orders (customer_id, order_id, created_at, total, status) VALUES (
  2f1a9c34-7b8d-4f21-8a0b-1234567890ab,
  6ba7b812-9dad-11d1-80b4-00c04fd430c8,
  '2024-02-20 09:00:00+0000',
  349.99,
  'paid'
);

-- Order items (Tron-flavored SKUs)
-- Flynn order #1 (6ba7b810-...)
INSERT INTO order_items (order_id, line_item, sku, qty, price)
VALUES (6ba7b810-9dad-11d1-80b4-00c04fd430c8, 1, 'IDENTITY-DISK-MK2', 1, 99.99);
INSERT INTO order_items (order_id, line_item, sku, qty, price)
VALUES (6ba7b810-9dad-11d1-80b4-00c04fd430c8, 2, 'LIGHTCYCLE-DECAL-TRON', 1, 29.96);

-- Flynn order #2 (6ba7b811-...)
INSERT INTO order_items (order_id, line_item, sku, qty, price)
VALUES (6ba7b811-9dad-11d1-80b4-00c04fd430c8, 1, 'RECOGNIZER-MINI', 2, 29.99);

-- Bradley order (6ba7b812-...)
INSERT INTO order_items (order_id, line_item, sku, qty, price)
VALUES (6ba7b812-9dad-11d1-80b4-00c04fd430c8, 1, 'LIGHTSUIT-CIRCUIT-UPGRADE', 1, 199.99);
INSERT INTO order_items (order_id, line_item, sku, qty, price)
VALUES (6ba7b812-9dad-11d1-80b4-00c04fd430c8, 2, 'SOLAR-SAILER-MODEL', 1, 149.99);

-- Done. Have fun on the Grid.
