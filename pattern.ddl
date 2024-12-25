-- Создание DIM таблиц

-- Создание таблицы {DIM_terminals}
CREATE TABLE IF NOT EXISTS {DIM_terminals} (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(3),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы {DIM_clients}
CREATE TABLE IF NOT EXISTS {DIM_clients} (
    client_id VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE, 
    passport_num VARCHAR(15) UNIQUE,
    passport_valid_to DATE, 
    phone VARCHAR(16) UNIQUE,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы {DIM_accounts}
CREATE TABLE IF NOT EXISTS {DIM_accounts} (
    account_num VARCHAR(20) PRIMARY KEY,
    valid_to DATE, 
    client VARCHAR(10) REFERENCES {DIM_clients}(client_id),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы {DIM_cards}
CREATE TABLE IF NOT EXISTS {DIM_cards} (
    cards_num VARCHAR(20) PRIMARY KEY,
    account_num VARCHAR(20) REFERENCES {DIM_accounts}(account_num),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание FACT таблиц

-- Создание таблицы {FACT_transactions}
CREATE TABLE IF NOT EXISTS {FACT_transactions} (
    trans_id VARCHAR(11) PRIMARY KEY,
    trans_date TIMESTAMP, 
    card_num VARCHAR(20) REFERENCES {DIM_cards}(cards_num),
    oper_type VARCHAR(8),
    amt DECIMAL,
    oper_result VARCHAR(7),
    terminal VARCHAR(5) REFERENCES {DIM_terminals}(terminal_id)
);

-- Создание таблицы {FACT_blacklist}
CREATE TABLE IF NOT EXISTS {FACT_blacklist} (
    passport_num VARCHAR(15),
    entry_dt DATE,
    PRIMARY KEY (passport_num, entry_dt)
);

-- Создание таблицы {REP_fraud}
CREATE TABLE IF NOT EXISTS {REP_fraud} (
    event_dt TIMESTAMP, 
    passport VARCHAR(15) REFERENCES {DIM_clients}(passport_num),
    fio VARCHAR(65),
    phone VARCHAR(16) REFERENCES {DIM_clients}(phone),
    event_type VARCHAR(20),
    report_dt TIMESTAMP 
);

-- Создание STG таблиц

-- Таблица {STG_transactions}
CREATE TABLE IF NOT EXISTS {STG_transactions} (
    transaction_id VARCHAR PRIMARY KEY,
    transaction_date TIMESTAMP, 
    amount DECIMAL,
    card_num VARCHAR,
    oper_type VARCHAR,
    oper_result VARCHAR,
    terminal VARCHAR
);

-- Таблица {STG_terminals}
CREATE TABLE IF NOT EXISTS {STG_terminals} (
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    date DATE
);

-- Таблица {STG_blacklist}
CREATE TABLE IF NOT EXISTS {STG_blacklist} (
    date DATE,
    passport VARCHAR
);

-- Таблица {STG_clients}
CREATE TABLE IF NOT EXISTS {STG_clients} (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patronymic VARCHAR,
    date_of_birth DATE,  
    passport_num VARCHAR UNIQUE, 
    passport_valid_to DATE,   
    phone VARCHAR UNIQUE,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица {STG_accounts}
CREATE TABLE IF NOT EXISTS {STG_accounts} (
    account VARCHAR PRIMARY KEY,
    valid_to DATE,   
    client VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица {STG_cards}
CREATE TABLE IF NOT EXISTS {STG_cards} (
    card_num VARCHAR PRIMARY KEY,
    account VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);
