CREATE TABLE LOGS.ETL_LOGS (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED')),
    rows_processed INTEGER,
    error_message TEXT
);