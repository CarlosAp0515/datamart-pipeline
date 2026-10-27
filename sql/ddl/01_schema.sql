CREATE SCHEMA IF NOT EXISTS dw;

-- ============ DIMENSIONES ============
CREATE TABLE dw.dim_product (
    product_code                    VARCHAR(20)  PRIMARY KEY,
    product_name                    VARCHAR(255),
    category                        VARCHAR(50)  NOT NULL,
    raw_description_variants        INT          NOT NULL DEFAULT 1,
    normalized_description_variants INT          NOT NULL DEFAULT 1
);

CREATE TABLE dw.dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    country     VARCHAR(100)
);

-- ============ HECHOS ============
-- Ventas validas: cantidad > 0 y precio > 0
CREATE TABLE dw.fact_sales (
    sale_id        BIGSERIAL     PRIMARY KEY,
    invoice_no     VARCHAR(20)   NOT NULL,
    product_code   VARCHAR(20)   NOT NULL REFERENCES dw.dim_product(product_code),
    customer_id    VARCHAR(20)   REFERENCES dw.dim_customer(customer_id), -- NULL = sin cliente
    country        VARCHAR(100),
    invoice_ts_utc TIMESTAMPTZ   NOT NULL,
    invoice_date   DATE          NOT NULL,
    quantity       INT           NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12,4) NOT NULL CHECK (unit_price > 0),
    gross_revenue  NUMERIC(14,4) NOT NULL,
    source_system  VARCHAR(30)   NOT NULL
);

-- Devoluciones / ajustes: cantidad <= 0
CREATE TABLE dw.fact_returns (
    return_id      BIGSERIAL     PRIMARY KEY,
    invoice_no     VARCHAR(20)   NOT NULL,
    product_code   VARCHAR(20)   NOT NULL REFERENCES dw.dim_product(product_code),
    customer_id    VARCHAR(20)   REFERENCES dw.dim_customer(customer_id),
    country        VARCHAR(100),
    invoice_ts_utc TIMESTAMPTZ   NOT NULL,
    invoice_date   DATE          NOT NULL,
    quantity       INT           NOT NULL CHECK (quantity <= 0),
    unit_price     NUMERIC(12,4) NOT NULL CHECK (unit_price > 0),
    return_amount  NUMERIC(14,4) NOT NULL,   -- ABS(quantity) * unit_price
    source_system  VARCHAR(30)   NOT NULL
);

-- Agregado diario por producto: bruto, devoluciones y neto
CREATE TABLE dw.agg_daily_product_net (
    invoice_date   DATE          NOT NULL,
    product_code   VARCHAR(20)   NOT NULL REFERENCES dw.dim_product(product_code),
    units_sold     INT           NOT NULL DEFAULT 0,
    gross_revenue  NUMERIC(16,4) NOT NULL DEFAULT 0,
    units_returned INT           NOT NULL DEFAULT 0,
    returned_amount NUMERIC(16,4) NOT NULL DEFAULT 0,
    net_revenue    NUMERIC(16,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (invoice_date, product_code)
);

-- ============ LOG DE RECHAZOS ============
CREATE TABLE dw.rejected_records (
    reject_id     BIGSERIAL    PRIMARY KEY,
    source_system VARCHAR(30)  NOT NULL,
    reason        VARCHAR(100) NOT NULL,
    raw_data      JSONB,
    rejected_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Indices utiles para las consultas de negocio
CREATE INDEX idx_sales_date      ON dw.fact_sales(invoice_date);
CREATE INDEX idx_sales_product   ON dw.fact_sales(product_code);
CREATE INDEX idx_returns_product ON dw.fact_returns(product_code);
CREATE INDEX idx_rejected_reason ON dw.rejected_records(reason);