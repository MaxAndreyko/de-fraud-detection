-- Создание DIM таблиц

-- Создание таблицы public.maka_dwh_dim_terminals_hist
CREATE TABLE IF NOT EXISTS public.maka_dwh_dim_terminals_hist (
    terminal_id VARCHAR(5),
    terminal_type VARCHAR(3),
    terminal_city VARCHAR(20),
    terminal_address VARCHAR(50),
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы public.maka_dwh_dim_clients_hist
CREATE TABLE IF NOT EXISTS public.maka_dwh_dim_clients_hist (
    client_id VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE, 
    passport_num VARCHAR(15) UNIQUE,
    passport_valid_to DATE, 
    phone VARCHAR(16) UNIQUE,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы public.maka_dwh_dim_accounts_hist
CREATE TABLE IF NOT EXISTS public.maka_dwh_dim_accounts_hist (
    account_num VARCHAR(20) PRIMARY KEY,
    valid_to DATE, 
    client VARCHAR(10) REFERENCES public.maka_dwh_dim_clients_hist(client_id),
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание таблицы public.maka_dwh_dim_cards_hist
CREATE TABLE IF NOT EXISTS public.maka_dwh_dim_cards_hist (
    cards_num VARCHAR(20) PRIMARY KEY,
    account_num VARCHAR(20) REFERENCES public.maka_dwh_dim_accounts_hist(account_num),
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Создание FACT таблиц

-- Создание таблицы public.maka_dwh_fact_transactions
CREATE TABLE IF NOT EXISTS public.maka_dwh_fact_transactions (
    trans_id VARCHAR(11) PRIMARY KEY,
    trans_date TIMESTAMP, 
    card_num VARCHAR(20) REFERENCES public.maka_dwh_dim_cards_hist(cards_num),
    oper_type VARCHAR(8),
    amt DECIMAL,
    oper_result VARCHAR(7),
    terminal VARCHAR(5) REFERENCES public.maka_dwh_dim_terminals_hist(terminal_id)
);

-- Создание таблицы public.maka_dwh_fact_passport_blacklist
CREATE TABLE IF NOT EXISTS public.maka_dwh_fact_passport_blacklist (
    passport_num VARCHAR(15),
    entry_dt DATE,
    PRIMARY KEY (passport_num, entry_dt)
);

-- Создание таблицы public.maka_rep_fraud
CREATE TABLE IF NOT EXISTS public.maka_rep_fraud (
    event_dt TIMESTAMP, 
    passport VARCHAR(15) REFERENCES public.maka_dwh_dim_clients_hist(passport_num),
    fio VARCHAR(65),
    phone VARCHAR(16) REFERENCES public.maka_dwh_dim_clients_hist(phone),
    event_type VARCHAR(20),
    report_dt TIMESTAMP 
);

-- Создание STG таблиц

-- Таблица public.maka_stg_transactions
CREATE TABLE IF NOT EXISTS public.maka_stg_transactions (
    transaction_id VARCHAR PRIMARY KEY,
    transaction_date TIMESTAMP, 
    amount DECIMAL,
    card_num VARCHAR,
    oper_type VARCHAR,
    oper_result VARCHAR,
    terminal VARCHAR
);

-- Таблица public.maka_stg_terminals
CREATE TABLE IF NOT EXISTS public.maka_stg_terminals (
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    date DATE
);

-- Таблица public.maka_stg_blacklist
CREATE TABLE IF NOT EXISTS public.maka_stg_blacklist (
    date DATE,
    passport VARCHAR
);

-- Таблица public.maka_stg_clients
CREATE TABLE IF NOT EXISTS public.maka_stg_clients (
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

-- Таблица public.maka_stg_accounts
CREATE TABLE IF NOT EXISTS public.maka_stg_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,   
    client VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Таблица public.maka_stg_cards
CREATE TABLE IF NOT EXISTS public.maka_stg_cards (
    card_num VARCHAR PRIMARY KEY,
    account VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP
);
