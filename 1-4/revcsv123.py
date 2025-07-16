import psycopg2
import csv
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'database': 'DS',
    'user': 'postgres',
    'password': '123'
}

def clean_numeric(value):
    """Преобразует пустые строки в NULL для numeric полей"""
    return None if value == '' else value

def import_from_csv(filename):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Создаем копию таблицы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (LIKE dm.dm_f101_round_f INCLUDING ALL)
        """)
        
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            # Определяем numeric колонки (должны соответствовать вашей структуре таблицы)
            numeric_columns = {
                'balance_in_rub', 'r_balance_in_rub',
                'balance_in_val', 'r_balance_in_val',
                'balance_in_total', 'r_balance_in_total',
                'turn_deb_rub', 'r_turn_deb_rub',
                'turn_deb_val', 'r_turn_deb_val',
                'turn_deb_total', 'r_turn_deb_total',
                'turn_cre_rub', 'r_turn_cre_rub',
                'turn_cre_val', 'r_turn_cre_val',
                'turn_cre_total', 'r_turn_cre_total',
                'balance_out_rub', 'r_balance_out_rub',
                'balance_out_val', 'r_balance_out_val',
                'balance_out_total', 'r_balance_out_total'
            }
            
            # Подготавливаем запрос
            columns = ', '.join(headers)
            placeholders = ', '.join(['%s'] * len(headers))
            query = f"INSERT INTO dm.dm_f101_round_f_v2 ({columns}) VALUES ({placeholders})"
            
            # Обрабатываем каждую строку
            count = 0
            for row in reader:
                # Обрабатываем numeric поля
                cleaned_row = [
                    clean_numeric(value) if headers[i].lower() in numeric_columns else value
                    for i, value in enumerate(row)
                ]
                
                cursor.execute(query, cleaned_row)
                count += 1
            
            conn.commit()
        
        log_message = f"Успешно загружено {count} записей из {filename}"
        print(log_message)
        log_to_db('IMPORT', 'SUCCESS', log_message)
        
    except Exception as e:
        error_msg = f"Ошибка при загрузке: {str(e)}"
        print(error_msg)
        log_to_db('IMPORT', 'FAILED', error_msg)
        raise
    finally:
        if conn:
            conn.close()

def log_to_db(action, status, message):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO logs.etl_logs (process_name, start_time, status, error_message) VALUES (%s, %s, %s, %s)",
            (f"CSV_{action}", datetime.now(), status, message)
        )
        conn.commit()
    except Exception as e:
        print(f"Ошибка логирования: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    filename = input("Введите имя CSV файла для загрузки: ")
    import_from_csv(filename)