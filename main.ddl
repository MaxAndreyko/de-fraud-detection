-- Создание DIM таблиц

-- Создание таблицы {DIM_terminals}
CREATE TABLE IF NOT EXISTS {DIM_terminals} (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(3),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Создание таблицы {DIM_clients}
CREATE TABLE IF NOT EXISTS {DIM_clients} (
    client_id VARCHAR(10),
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE, 
    passport_num VARCHAR(15),
    passport_valid_to DATE, 
    phone VARCHAR(16),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Создание таблицы {DIM_accounts}
CREATE TABLE IF NOT EXISTS {DIM_accounts} (
    account_num VARCHAR(20),
    valid_to DATE, 
    client VARCHAR(10),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Создание таблицы {DIM_cards}
CREATE TABLE IF NOT EXISTS {DIM_cards} (
    cards_num VARCHAR(20),
    account_num VARCHAR(20),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
);

-- Создание FACT таблиц

-- Создание таблицы {FACT_transactions}
CREATE TABLE IF NOT EXISTS {FACT_transactions} (
    trans_id VARCHAR(11) PRIMARY KEY,
    trans_date TIMESTAMP, 
    card_num VARCHAR(20),
    oper_type VARCHAR(8),
    amt DECIMAL,
    oper_result VARCHAR(7),
    terminal VARCHAR(5)
);

-- Создание таблицы {FACT_blacklist}
CREATE TABLE IF NOT EXISTS {FACT_blacklist} (
    passport_num VARCHAR(15),
    entry_dt DATE
);

-- Создание таблицы {REP_fraud}
CREATE TABLE IF NOT EXISTS {REP_fraud} (
    event_dt TIMESTAMP, 
    passport VARCHAR(15),
    fio VARCHAR(65),
    phone VARCHAR(16),
    event_type VARCHAR(50),
    report_dt TIMESTAMP 
);

-- Создание STG таблиц

-- Таблица {STG_transactions}
CREATE TABLE IF NOT EXISTS {STG_transactions} (
    transaction_id VARCHAR(11) PRIMARY KEY,
    transaction_date TIMESTAMP, 
    amount DECIMAL,
    card_num VARCHAR(20),
    oper_type VARCHAR(8),
    oper_result VARCHAR(7),
    terminal VARCHAR(5)
);

-- Таблица {STG_terminals}
CREATE TABLE IF NOT EXISTS {STG_terminals} (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(3),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    date DATE
);

-- Таблица {STG_blacklist}
CREATE TABLE IF NOT EXISTS {STG_blacklist} (
    date DATE,
    passport VARCHAR(15)
);

-- Таблица {STG_clients}
CREATE TABLE IF NOT EXISTS {STG_clients} (
    client_id VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,  
    passport_num VARCHAR(15), 
    passport_valid_to DATE,   
    phone VARCHAR(16),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица {STG_accounts}
CREATE TABLE IF NOT EXISTS {STG_accounts} (
    account VARCHAR(20) PRIMARY KEY,
    valid_to DATE,   
    client VARCHAR(10),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица {STG_cards}
CREATE TABLE IF NOT EXISTS {STG_cards} (
    card_num VARCHAR(20) PRIMARY KEY,
    account VARCHAR(20),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Создание таблицы метаданных

CREATE TABLE IF NOT EXISTS {META} (
    table_name VARCHAR(30),
    max_update_dt TIMESTAMP(0)
);

-- Вставка значений по-умолчанию в таблицу метаданных

INSERT INTO {META} (table_name, max_update_dt)
SELECT new_tables.table_name, to_timestamp('1900-01-01', 'YYYY-MM-DD')
FROM (
    SELECT '{STG_transactions}' AS table_name UNION ALL
    SELECT '{STG_terminals}' UNION ALL
    SELECT '{STG_blacklist}' UNION ALL
    SELECT '{STG_clients}' UNION ALL
    SELECT '{STG_accounts}' UNION ALL
    SELECT '{STG_cards}'
) AS new_tables
WHERE NOT EXISTS (
    SELECT 1 
    FROM {META} 
    WHERE table_name = new_tables.table_name
);
