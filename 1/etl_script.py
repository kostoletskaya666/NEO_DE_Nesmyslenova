import psycopg2
import pandas as pd
from datetime import datetime
import time
import os

def log_process(conn, cursor, process_name, status, rows_processed=None, error=None):
    cursor.execute("""
        INSERT INTO LOGS.ETL_LOGS 
        (process_name, start_time, status, rows_processed, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (process_name, datetime.now(), status, rows_processed, error))
    conn.commit()

def parse_custom_date(date_str):
    """Парсинг даты в разных форматах"""
    try:
        return pd.to_datetime(date_str, format='%d.%m.%Y', dayfirst=True)
    except ValueError:
        try:
            return pd.to_datetime(date_str, format='%d-%m-%Y', dayfirst=True)
        except ValueError:
            return pd.to_datetime(date_str)

def load_csv_to_table(conn, cursor, csv_path, table_name, date_columns=None):
    try:
        log_process(conn, cursor, table_name, 'STARTED')
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл {csv_path} не найден")
        
        # Читаем CSV с разделителем ;
        df = pd.read_csv(csv_path, sep=';')
        print("Фактические колонки в CSV:", df.columns.tolist())
        
        # Обработка дат
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].apply(parse_custom_date)
                else:
                    raise ValueError(f"Колонка {col} не найдена в файле")
        
        # Очистка таблицы перед загрузкой
        if table_name in ["DS.FT_POSTING_F", "DS.MD_LEDGER_ACCOUNT_S"]:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
        
        # Загрузка данных
        rows_processed = 0
        for _, row in df.iterrows():
            try:
                if table_name == "DS.FT_BALANCE_F":
                    cursor.execute(f"""
                        INSERT INTO {table_name} (on_date, account_rk, currency_rk, balance_out)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (on_date, account_rk) 
                        DO UPDATE SET 
                            currency_rk = EXCLUDED.currency_rk,
                            balance_out = EXCLUDED.balance_out
                    """, (
                        row['ON_DATE'],
                        row['ACCOUNT_RK'],
                        row.get('CURRENCY_RK'),
                        row.get('BALANCE_OUT')
                    ))
                
                elif table_name == "DS.FT_POSTING_F":
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (oper_date, credit_account_rk, debit_account_rk, credit_amount, debit_amount)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        row['OPER_DATE'],
                        row['CREDIT_ACCOUNT_RK'],
                        row['DEBET_ACCOUNT_RK'],
                        row.get('CREDIT_AMOUNT', 0),
                        row.get('DEBET_AMOUNT', 0)
                    ))
                
                elif table_name == "DS.MD_ACCOUNT_D":
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (data_actual_date, data_actual_end_date, account_rk, account_number, 
                         char_type, currency_rk, currency_code)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (data_actual_date, account_rk) 
                        DO UPDATE SET
                            data_actual_end_date = EXCLUDED.data_actual_end_date,
                            account_number = EXCLUDED.account_number,
                            char_type = EXCLUDED.char_type,
                            currency_rk = EXCLUDED.currency_rk,
                            currency_code = EXCLUDED.currency_code
                    """, (
                        row['DATA_ACTUAL_DATE'],
                        row['DATA_ACTUAL_END_DATE'],
                        row['ACCOUNT_RK'],
                        row['ACCOUNT_NUMBER'],
                        row['CHAR_TYPE'],
                        row['CURRENCY_RK'],
                        row['CURRENCY_CODE']
                    ))
                
                elif table_name == "DS.MD_CURRENCY_D":
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (currency_rk, data_actual_date, data_actual_end_date, 
                         currency_code, code_iso_char)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (currency_rk, data_actual_date) 
                        DO UPDATE SET
                            data_actual_end_date = EXCLUDED.data_actual_end_date,
                            currency_code = EXCLUDED.currency_code,
                            code_iso_char = EXCLUDED.code_iso_char
                    """, (
                        row['CURRENCY_RK'],
                        row['DATA_ACTUAL_DATE'],
                        row.get('DATA_ACTUAL_END_DATE'),
                        row['CURRENCY_CODE'],
                        row.get('CODE_ISO_CHAR')
                    ))
                
                elif table_name == "DS.MD_EXCHANGE_RATE_D":
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (data_actual_date, data_actual_end_date, currency_rk, 
                         reduced_cource, code_iso_num)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (data_actual_date, currency_rk) 
                        DO UPDATE SET
                            data_actual_end_date = EXCLUDED.data_actual_end_date,
                            reduced_cource = EXCLUDED.reduced_cource,
                            code_iso_num = EXCLUDED.code_iso_num
                    """, (
                        row['DATA_ACTUAL_DATE'],
                        row.get('DATA_ACTUAL_END_DATE'),
                        row['CURRENCY_RK'],
                        row['REDUCED_COURCE'],
                        row.get('CODE_ISO_NUM')
                    ))
                
                elif table_name == "DS.MD_LEDGER_ACCOUNT_S":
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (chapter, chapter_name, section_number, section_name,
                         subsection_name, ledger1_account, ledger1_account_name,
                         ledger_account, ledger_account_name, characteristic,
                         is_resident, is_reserve, is_reserved, is_loan,
                         is_reserved_assets, is_overdue, is_interest, pair_account,
                         start_date, end_date, is_rub_only, min_term,
                         min_term_measure, max_term, max_term_measure,
                         ledger_acc_full_name_translit, is_revaluation, is_correct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ledger_account, start_date) 
                        DO UPDATE SET
                            chapter = EXCLUDED.chapter,
                            chapter_name = EXCLUDED.chapter_name,
                            section_number = EXCLUDED.section_number,
                            section_name = EXCLUDED.section_name,
                            subsection_name = EXCLUDED.subsection_name,
                            ledger1_account = EXCLUDED.ledger1_account,
                            ledger1_account_name = EXCLUDED.ledger1_account_name,
                            ledger_account_name = EXCLUDED.ledger_account_name,
                            characteristic = EXCLUDED.characteristic,
                            is_resident = EXCLUDED.is_resident,
                            is_reserve = EXCLUDED.is_reserve,
                            is_reserved = EXCLUDED.is_reserved,
                            is_loan = EXCLUDED.is_loan,
                            is_reserved_assets = EXCLUDED.is_reserved_assets,
                            is_overdue = EXCLUDED.is_overdue,
                            is_interest = EXCLUDED.is_interest,
                            pair_account = EXCLUDED.pair_account,
                            end_date = EXCLUDED.end_date,
                            is_rub_only = EXCLUDED.is_rub_only,
                            min_term = EXCLUDED.min_term,
                            min_term_measure = EXCLUDED.min_term_measure,
                            max_term = EXCLUDED.max_term,
                            max_term_measure = EXCLUDED.max_term_measure,
                            ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit,
                            is_revaluation = EXCLUDED.is_revaluation,
                            is_correct = EXCLUDED.is_correct
                    """, (
                        row.get('CHAPTER'),
                        row.get('CHAPTER_NAME'),
                        row.get('SECTION_NUMBER'),
                        row.get('SECTION_NAME'),
                        row.get('SUBSECTION_NAME'),
                        row.get('LEDGER1_ACCOUNT'),
                        row.get('LEDGER1_ACCOUNT_NAME'),
                        row['LEDGER_ACCOUNT'],
                        row.get('LEDGER_ACCOUNT_NAME'),
                        row.get('CHARACTERISTIC'),
                        row.get('IS_RESIDENT'),
                        row.get('IS_RESERVE'),
                        row.get('IS_RESERVED'),
                        row.get('IS_LOAN'),
                        row.get('IS_RESERVED_ASSETS'),
                        row.get('IS_OVERDUE'),
                        row.get('IS_INTEREST'),
                        row.get('PAIR_ACCOUNT'),
                        row['START_DATE'],
                        row.get('END_DATE'),
                        row.get('IS_RUB_ONLY'),
                        row.get('MIN_TERM'),
                        row.get('MIN_TERM_MEASURE'),
                        row.get('MAX_TERM'),
                        row.get('MAX_TERM_MEASURE'),
                        row.get('LEDGER_ACC_FULL_NAME_TRANSLIT'),
                        row.get('IS_REVALUATION'),
                        row.get('IS_CORRECT')
                    ))
                
                rows_processed += 1
            
            except Exception as e:
                print(f"Ошибка при обработке строки {_}: {str(e)}")
                continue
        
        log_process(conn, cursor, table_name, 'SUCCESS', rows_processed)
        print(f"Успешно загружено {rows_processed} строк в {table_name}")
    
    except Exception as e:
        log_process(conn, cursor, table_name, 'FAILED', error=str(e))
        print(f"Ошибка при загрузке в {table_name}: {str(e)}")
        raise

def main():
    try:
        conn = psycopg2.connect(
            dbname="DS",
            user="postgres",
            password="123",
            host="localhost"
        )
        cursor = conn.cursor()
        
        print("Начало ETL-процесса...")
       
        
        # Загрузка всех таблиц слоя DS
        load_csv_to_table(conn, cursor, "1/data/ft_balance_f.csv", "DS.FT_BALANCE_F", date_columns=['ON_DATE'])
        load_csv_to_table(conn, cursor, "1/data/ft_posting_f.csv", "DS.FT_POSTING_F", date_columns=['OPER_DATE'])
        load_csv_to_table(conn, cursor, "1/data/md_account_d.csv", "DS.MD_ACCOUNT_D", date_columns=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'])
        load_csv_to_table(conn, cursor, "1/data/md_currency_d.csv", "DS.MD_CURRENCY_D", date_columns=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'])
        load_csv_to_table(conn, cursor, "1/data/md_exchange_rate_d.csv", "DS.MD_EXCHANGE_RATE_D", date_columns=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'])
        load_csv_to_table(conn, cursor, "1/data/md_ledger_account_s.csv", "DS.MD_LEDGER_ACCOUNT_S", date_columns=['START_DATE', 'END_DATE'])
        time.sleep(5)
        
    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("ETL-процесс завершен")

if __name__ == "__main__":
    main()