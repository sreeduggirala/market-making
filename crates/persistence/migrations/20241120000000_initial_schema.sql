-- Initial schema for market making system
-- Creates tables for orders, fills, positions, PnL tracking, and risk events

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- ENUMS
-- ============================================================================

CREATE TYPE exchange_type AS ENUM ('kraken', 'mexc', 'bybit');
CREATE TYPE order_side AS ENUM ('buy', 'sell');
CREATE TYPE order_type AS ENUM ('limit', 'market', 'stop_loss', 'stop_loss_limit', 'take_profit', 'take_profit_limit');
CREATE TYPE order_status AS ENUM ('new', 'partially_filled', 'filled', 'canceled', 'rejected', 'expired');
CREATE TYPE time_in_force AS ENUM ('gtc', 'ioc', 'fok');
CREATE TYPE risk_severity AS ENUM ('info', 'warning', 'critical', 'emergency');

-- ============================================================================
-- ORDERS TABLE
-- ============================================================================

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identifiers
    client_order_id VARCHAR(64) NOT NULL,
    venue_order_id VARCHAR(64),

    -- Exchange and symbol
    exchange exchange_type NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    -- Order details
    side order_side NOT NULL,
    order_type order_type NOT NULL,
    time_in_force time_in_force,

    -- Quantities and prices
    quantity DECIMAL(24, 12) NOT NULL,
    price DECIMAL(24, 12),
    stop_price DECIMAL(24, 12),

    -- Fill tracking
    filled_quantity DECIMAL(24, 12) NOT NULL DEFAULT 0,
    remaining_quantity DECIMAL(24, 12) NOT NULL,
    average_fill_price DECIMAL(24, 12),

    -- Status
    status order_status NOT NULL DEFAULT 'new',

    -- Flags
    post_only BOOLEAN NOT NULL DEFAULT FALSE,
    reduce_only BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMPTZ,
    filled_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,

    -- Exchange timestamps (milliseconds)
    exchange_created_ms BIGINT,
    exchange_updated_ms BIGINT,

    -- Strategy reference
    strategy_id VARCHAR(64),

    -- Raw exchange response (for debugging)
    raw_status VARCHAR(64),

    UNIQUE(exchange, client_order_id)
);

-- Indexes for orders
CREATE INDEX idx_orders_exchange_symbol ON orders(exchange, symbol);
CREATE INDEX idx_orders_status ON orders(status) WHERE status IN ('new', 'partially_filled');
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_orders_strategy_id ON orders(strategy_id) WHERE strategy_id IS NOT NULL;
CREATE INDEX idx_orders_venue_order_id ON orders(venue_order_id) WHERE venue_order_id IS NOT NULL;

-- ============================================================================
-- FILLS TABLE
-- ============================================================================

CREATE TABLE fills (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Order reference
    order_id UUID REFERENCES orders(id),

    -- Identifiers
    venue_order_id VARCHAR(64) NOT NULL,
    client_order_id VARCHAR(64) NOT NULL,
    execution_id VARCHAR(64) NOT NULL,

    -- Exchange and symbol
    exchange exchange_type NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    -- Fill details
    side order_side NOT NULL,
    price DECIMAL(24, 12) NOT NULL,
    quantity DECIMAL(24, 12) NOT NULL,

    -- Fees
    fee DECIMAL(24, 12) NOT NULL DEFAULT 0,
    fee_currency VARCHAR(16),

    -- Maker/taker
    is_maker BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange_ts_ms BIGINT NOT NULL,
    received_ts_ms BIGINT NOT NULL,

    -- Calculated fields
    notional_value DECIMAL(24, 12) GENERATED ALWAYS AS (price * quantity) STORED,

    UNIQUE(exchange, execution_id)
);

-- Indexes for fills
CREATE INDEX idx_fills_order_id ON fills(order_id);
CREATE INDEX idx_fills_exchange_symbol ON fills(exchange, symbol);
CREATE INDEX idx_fills_created_at ON fills(created_at DESC);
CREATE INDEX idx_fills_exchange_ts ON fills(exchange_ts_ms DESC);

-- ============================================================================
-- POSITIONS TABLE
-- ============================================================================

CREATE TABLE positions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Exchange and symbol
    exchange exchange_type NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    -- Position details
    quantity DECIMAL(24, 12) NOT NULL,
    entry_price DECIMAL(24, 12) NOT NULL,
    mark_price DECIMAL(24, 12),
    liquidation_price DECIMAL(24, 12),

    -- PnL
    unrealized_pnl DECIMAL(24, 12),
    realized_pnl DECIMAL(24, 12) DEFAULT 0,

    -- Margin (for perps)
    margin DECIMAL(24, 12),
    leverage INTEGER,

    -- Timestamps
    opened_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,

    -- Status
    is_open BOOLEAN NOT NULL DEFAULT TRUE,

    UNIQUE(exchange, symbol) -- Only one open position per exchange/symbol
);

-- Indexes for positions
CREATE INDEX idx_positions_open ON positions(exchange, symbol) WHERE is_open = TRUE;
CREATE INDEX idx_positions_updated_at ON positions(updated_at DESC);

-- ============================================================================
-- POSITION HISTORY TABLE (snapshots)
-- ============================================================================

CREATE TABLE position_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Position reference
    position_id UUID REFERENCES positions(id),

    -- Exchange and symbol
    exchange exchange_type NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    -- Snapshot data
    quantity DECIMAL(24, 12) NOT NULL,
    entry_price DECIMAL(24, 12) NOT NULL,
    mark_price DECIMAL(24, 12),
    unrealized_pnl DECIMAL(24, 12),
    realized_pnl DECIMAL(24, 12),

    -- Snapshot timestamp
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for position history
CREATE INDEX idx_position_history_position_id ON position_history(position_id);
CREATE INDEX idx_position_history_snapshot_at ON position_history(snapshot_at DESC);

-- ============================================================================
-- PNL DAILY TABLE
-- ============================================================================

CREATE TABLE pnl_daily (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Date
    date DATE NOT NULL,

    -- Exchange (NULL for aggregate)
    exchange exchange_type,

    -- Symbol (NULL for aggregate)
    symbol VARCHAR(32),

    -- PnL breakdown
    realized_pnl DECIMAL(24, 12) NOT NULL DEFAULT 0,
    unrealized_pnl DECIMAL(24, 12) NOT NULL DEFAULT 0,
    fees_paid DECIMAL(24, 12) NOT NULL DEFAULT 0,

    -- Volume
    volume_traded DECIMAL(24, 12) NOT NULL DEFAULT 0,
    num_trades INTEGER NOT NULL DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(date, exchange, symbol)
);

-- Index for PnL daily
CREATE INDEX idx_pnl_daily_date ON pnl_daily(date DESC);

-- ============================================================================
-- RISK EVENTS TABLE
-- ============================================================================

CREATE TABLE risk_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Event details
    severity risk_severity NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,

    -- Context
    exchange exchange_type,
    symbol VARCHAR(32),
    order_id UUID REFERENCES orders(id),

    -- Violation details (JSON for flexibility)
    violation_details JSONB,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange_ts_ms BIGINT,

    -- Resolution
    resolved_at TIMESTAMPTZ,
    resolution_notes TEXT
);

-- Indexes for risk events
CREATE INDEX idx_risk_events_severity ON risk_events(severity);
CREATE INDEX idx_risk_events_created_at ON risk_events(created_at DESC);
CREATE INDEX idx_risk_events_unresolved ON risk_events(created_at DESC) WHERE resolved_at IS NULL;

-- ============================================================================
-- KILL SWITCH EVENTS TABLE
-- ============================================================================

CREATE TABLE kill_switch_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Trigger details
    trigger_type VARCHAR(32) NOT NULL,
    reason TEXT NOT NULL,

    -- Timestamps
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reset_at TIMESTAMPTZ,

    -- Actions taken
    orders_canceled INTEGER,
    positions_flattened BOOLEAN DEFAULT FALSE,

    -- Operator
    reset_by VARCHAR(64)
);

-- Index for kill switch events
CREATE INDEX idx_kill_switch_triggered_at ON kill_switch_events(triggered_at DESC);

-- ============================================================================
-- STRATEGY SESSIONS TABLE
-- ============================================================================

CREATE TABLE strategy_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Strategy identification
    strategy_name VARCHAR(64) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL,

    -- Configuration (JSON)
    config JSONB,

    -- Session timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,

    -- Performance summary
    total_orders INTEGER DEFAULT 0,
    total_fills INTEGER DEFAULT 0,
    total_volume DECIMAL(24, 12) DEFAULT 0,
    realized_pnl DECIMAL(24, 12) DEFAULT 0,

    -- Status
    exit_reason VARCHAR(64)
);

-- Index for strategy sessions
CREATE INDEX idx_strategy_sessions_started_at ON strategy_sessions(started_at DESC);
CREATE INDEX idx_strategy_sessions_strategy_name ON strategy_sessions(strategy_name);

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_positions_updated_at
    BEFORE UPDATE ON positions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pnl_daily_updated_at
    BEFORE UPDATE ON pnl_daily
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
