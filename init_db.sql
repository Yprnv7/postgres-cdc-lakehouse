-- PostgreSQL CDC Initialization Script
-- Configured for Debezium Logical Replication via pgoutput

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    tier VARCHAR(50) DEFAULT 'STANDARD',
    address_street VARCHAR(255),
    address_city VARCHAR(100),
    address_country VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Set REPLICA IDENTITY FULL so WAL emits complete before-and-after row images on UPDATE/DELETE
ALTER TABLE customers REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL REFERENCES customers(customer_id),
    status VARCHAR(50) NOT NULL,
    amount_cents BIGINT NOT NULL,
    currency VARCHAR(10) DEFAULT 'EUR',
    item_count INT DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE orders REPLICA IDENTITY FULL;

-- Seed baseline reference record
INSERT INTO customers (customer_id, name, email, phone, tier, address_street, address_city, address_country)
VALUES ('cust_init_001', 'Enterprise Baseline', 'compliance@enterprise.eu', '+353-1-496-0000', 'TIER_1', '1 Grand Canal Square', 'Dublin', 'Ireland')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, status, amount_cents, currency, item_count)
VALUES ('ord_init_001', 'cust_init_001', 'CREATED', 125000, 'EUR', 3)
ON CONFLICT (order_id) DO NOTHING;
