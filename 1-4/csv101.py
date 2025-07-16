import psycopg2
import csv
from datetime import datetime

# Настройки подключения
DB_CONFIG = {
    'host': 'localhost',
    'database': 'DS',
    'user': 'postgres',
    'password': '123'
}

def export_to_csv():
    try:
        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Создаем файл с timestamp в имени
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'dm_f101_round_f_export_{timestamp}.csv'
        
        # Выполняем запрос
        cursor.execute("SELECT * FROM dm.dm_f101_round_f")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        
        # Записываем в CSV
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(colnames)  # Заголовки
            writer.writerows(rows)
        
        # Логирование
        log_message = f"Успешная выгрузка {len(rows)} записей в {filename}"
        print(log_message)
        log_to_db('EXPORT', 'SUCCESS', log_message)
        
        return filename
        
    except Exception as e:
        error_msg = f"Ошибка при выгрузке: {str(e)}"
        print(error_msg)
        log_to_db('EXPORT', 'FAILED', error_msg)
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
    export_to_csv()