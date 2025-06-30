import psycopg2
import pandas as pd
from datetime import datetime
import time
import os

"""
ETL-скрипт для загрузки банковских данных из CSV в PostgreSQL
Обрабатывает файлы с разделителем ";" и разными форматами дат
"""

def log_process(conn, cursor, process_name, status, rows_processed=None, error=None):

    """
    Логирование этапов ETL-процесса в таблицу LOGS.ETL_LOGS
    
    Параметры:
    conn - соединение с БД
    cursor - курсор БД
    process_name - название процесса (имя таблицы)
    status - статус выполнения ('STARTED', 'SUCCESS', 'FAILED')
    rows_processed - количество обработанных строк
    error - текст ошибки
    """

    cursor.execute("""
        INSERT INTO LOGS.ETL_LOGS 
        (process_name, start_time, status, rows_processed, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (process_name, datetime.now(), status, rows_processed, error))
    conn.commit()

def parse_custom_date(date_str):
    """Парсинг даты в разных форматах"""
    try:
        # Пробуем формат день.месяц.год
        return pd.to_datetime(date_str, format='%d.%m.%Y', dayfirst=True)
    except ValueError:
        try:
            # Пробуем формат день-месяц-год
            return pd.to_datetime(date_str, format='%d-%m-%Y', dayfirst=True)
        except ValueError:
            # Пробуем автоматическое определение
            return pd.to_datetime(date_str)

def load_csv_to_table(conn, cursor, csv_path, table_name, date_columns=None):
    """
    Основная функция загрузки данных из CSV в таблицу PostgreSQL
    
    Параметры:
    conn - соединение с БД
    cursor - курсор БД
    csv_path - путь к CSV-файлу
    table_name - имя целевой таблицы
    date_columns - список колонок с датами для преобразования
    """
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
        if table_name == "DS.FT_POSTING_F":
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
        time.sleep(5)
        
        # Загрузка данных с указанием правильных колонок с датами
        load_csv_to_table(conn, cursor, "data/FT_BALANCE_F.csv", "DS.FT_BALANCE_F", date_columns=['ON_DATE'])
        load_csv_to_table(conn, cursor, "data/FT_POSTING_F.csv", "DS.FT_POSTING_F", date_columns=['OPER_DATE'])
        
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