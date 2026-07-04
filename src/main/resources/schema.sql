-- ============================================================================
-- PROJECT: TechMart Enterprise Modernization Platform
-- MODULE: Core Relational Data Tier Schema Specification
-- TARGET ENGINE: MySQL 8.0+ / Enterprise Cluster Configurations
-- COMPATIBILITY: Jakarta EE 9 connection-pooled DataSources
-- AUTHOR: Oshen Sathsara Hettiwana
-- ============================================================================

-- Create and isolate the modernized high-throughput database catalog
CREATE DATABASE IF NOT EXISTS techmart_modernization_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE techmart_modernization_db;

-- Disable foreign key constraints temporarily to facilitate clean re-initialization
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS warehouses;
SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- 1. Warehouses Dimension Table
-- Satisfies NFR: Centralized tracking profiles for real-time warehouse sync.
-- ============================================================================
CREATE TABLE warehouses (
    id          VARCHAR(50)  NOT NULL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    location    VARCHAR(255) NOT NULL,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_warehouse_status (is_active)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC COMMENT='Multi-warehouse logistics node tracking registry';

-- ============================================================================
-- 2. Products Inventory Catalogue Table
-- Optimizations:
--   - Authoritative transactional table layer.
--   - Memory state cached concurrently via InventoryTrackerBean.
--   - Composite indexing schemas tailored for 10,000+ concurrent users query loads.
-- ============================================================================
CREATE TABLE products (
    id             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sku            VARCHAR(50)    NOT NULL UNIQUE,
    name           VARCHAR(255)   NOT NULL,
    description    TEXT,
    price          DECIMAL(10,2)  NOT NULL DEFAULT 0.00,
    stock_quantity INT            NOT NULL DEFAULT 0,
    warehouse_id   VARCHAR(50)    NOT NULL,
    category       VARCHAR(100)   NOT NULL,
    created_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Strict foreign key constraint enforcing data integrity across nodes
    CONSTRAINT fk_products_warehouse
        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id) ON DELETE RESTRICT,

    -- High-performance transactional lookup indexes
    INDEX idx_products_category_name (category, name),
    INDEX idx_products_sku_lookup (sku),
    INDEX idx_products_pricing (price),

    -- Full-text optimization tree for sub-second keyword exploration pipelines
    FULLTEXT INDEX ft_products_fuzzy_search (name, description)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC COMMENT='Authoritative catalog table synchronized with EJB Singleton caching engine';

-- ============================================================================
-- 3. Orders Header Transaction Table
-- Optimizations:
--   - State boundaries strictly managed via database-tier checking.
--   - Partition-ready composite clustering layout.
-- ============================================================================
CREATE TABLE orders (
    id               BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id      VARCHAR(100)  NOT NULL,
    customer_email   VARCHAR(255)  NOT NULL,
    total_amount     DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    status           ENUM('PENDING','CONFIRMED','PROCESSING','SHIPPED','DELIVERED','CANCELLED')
                     NOT NULL DEFAULT 'PENDING',
    shipping_address TEXT          NOT NULL,
    placed_at        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    processing_notes TEXT,

    -- Composite query tree coverage for customer order tracking pages
    INDEX idx_orders_customer_date (customer_id, placed_at DESC),
    INDEX idx_orders_state_tracking (status),
    INDEX idx_orders_chronological (placed_at)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC COMMENT='E-commerce order transaction header registry';

-- ============================================================================
-- 4. Order Line Items Dependent Table
-- Optimizations:
--   - Cascading deletions managed via container boundaries.
--   - Structured index tracking protecting transaction logs under heavy load.
-- ============================================================================
CREATE TABLE order_items (
    id           BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id     BIGINT        NOT NULL,
    product_id   BIGINT        NOT NULL,
    product_sku  VARCHAR(50)   NOT NULL,
    product_name VARCHAR(255)  NOT NULL,
    quantity     INT           NOT NULL DEFAULT 1,
    unit_price   DECIMAL(10,2) NOT NULL,

    -- Referential constraint validations protecting historical metrics
    CONSTRAINT fk_items_order_header
        FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_items_product_catalog
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT,

    INDEX idx_line_items_parent (order_id),
    INDEX idx_line_items_product (product_id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC COMMENT='Granular purchase ledger record detailing inventory acquisitions';


-- ============================================================================
-- SAMPLE DATA INJECTION (MODERNIZED PLATFORM INITIALIZATION SEED)
-- ============================================================================

-- Seed core distributed multi-warehouse architecture boundaries
INSERT INTO warehouses (id, name, location, is_active) VALUES
('WH-NORTH', 'TechMart BCD Regional Hub - North', 'Sabaragamuwa Logistic District Node Alpha', 1),
('WH-EAST',  'TechMart BCD Regional Hub - East',  'Eastern Maritime Supply Depot Node Beta', 1),
('WH-SOUTH', 'TechMart BCD Regional Hub - South', 'Southern Industrial Corridor Node Gamma', 1),
('WH-WEST',  'TechMart BCD Regional Hub - West',  'Western Port Distribution Center Node Delta', 1);

-- Seed core enterprise product lines with normalized warehouse relationships
INSERT INTO products (sku, name, description, price, stock_quantity, warehouse_id, category) VALUES
('LAPTOP-001', 'TechMart ProBook 15 Premium', '15.6-inch Enterprise Edition Laptop, Core i7, 16GB DDR4 RAM, 512GB NVMe Storage, Windows 11 Pro', 1299.99, 85, 'WH-NORTH', 'Electronics'),
('LAPTOP-002', 'TechMart UltraSlim 13 Eco', '13.3-inch High-Mobility Ultrabook, AMD Ryzen 7 Mobile processor, 8GB LPDDR4X, 256GB SSD', 899.99, 140, 'WH-NORTH', 'Electronics'),
('PHONE-001', 'TechMart SmartX Pro Max 5G', '6.7-inch Super AMOLED Infinite Display, Extended Battery Life, 256GB Internal Flash, Ceramic Shell', 699.99, 340, 'WH-EAST', 'Mobile'),
('PHONE-002', 'TechMart SmartX Lite Essential', '6.1-inch Multi-Touch Display, 4G LTE High Stability Chipset, 128GB Storage Core Edition', 399.99, 210, 'WH-EAST', 'Mobile'),
('TAB-001', 'TechMart TabPro 10 High-Def', '10.5-inch Graphic Tablet, High-Throughput WiFi+5G Module, 128GB, Active Stylus Pen Included', 549.99, 110, 'WH-SOUTH', 'Tablets'),
('HDPHN-001', 'TechMart SoundMax Pro ANC', 'Audiophile Grade Wireless Active Noise Cancelling Headphones, Low-Latency 40hr Operating Cycle', 249.99, 195, 'WH-WEST', 'Audio'),
('MOUSE-001', 'TechMart ErgoMouse Wireless', 'Precision Ergonomic Optical Wireless Mouse, Dual-Channel 2.4GHz / Bluetooth Mesh Integration', 49.99, 450, 'WH-WEST', 'Accessories'),
('KBD-001', 'TechMart MechKey RGB Matrix', 'Tactile Mechanical Performance Keyboard, Premium Aircraft-Grade Chassis, Customizable RGB Profiles', 129.99, 115, 'WH-WEST', 'Accessories');