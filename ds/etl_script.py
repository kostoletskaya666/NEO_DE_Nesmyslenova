"""
ETL-скрипт для загрузки банковских данных из CSV в PostgreSQL
Обрабатывает файлы с разделителем ";" и разными форматами дат
"""

# Импорт необходимых библиотек
import psycopg2  # Для работы с PostgreSQL
import pandas as pd  # Для обработки CSV
from datetime import datetime  # Для работы с датами
import time  # Для пауз
import os  # Для работы с файловой системой

def log_process(conn, cursor, process_name, status, rows_processed=None, error=None):
    """
    Логирование этапов ETL-процесса в таблицу LOGS.ETL_LOGS
    
    Параметры:
    conn - соединение с БД
    cursor - курсор БД
    process_name - название процесса (имя таблицы)
    status - статус выполнения ('STARTED', 'SUCCESS', 'FAILED')
    rows_processed - количество обработанных строк
    error - текст ошибки (если есть)
    """
    cursor.execute("""
        INSERT INTO LOGS.ETL_LOGS 
        (process_name, start_time, status, rows_processed, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (process_name, datetime.now(), status, rows_processed, error))
    conn.commit()

def parse_custom_date(date_str):
    """
    Универсальный парсер дат из строки с обработкой разных форматов:
    - день.месяц.год (15.01.2018)
    - день-месяц-год (15-01-2018)
    - автоматическое определение формата
    
    Параметры:
    date_str - строка с датой
    
    Возвращает:
    Объект datetime
    """
    try:
        # Пробуем формат день.месяц.год
        return pd.to_datetime(date_str, format='%d.%m.%Y', dayfirst=True)
    except ValueError:
        try:
            # Пробуем формат день-месяц-год
            return pd.to_datetime(date_str, format='%d-%m-%Y', dayfirst=True)
        except ValueError:
            # Если не сработало - пробуем автоопределение
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
        # Логируем начало загрузки
        log_process(conn, cursor, table_name, 'STARTED')
        
        # Проверка существования файла
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл {csv_path} не найден")
        
        # Чтение CSV с разделителем ";" (точка с запятой)
        df = pd.read_csv(csv_path, sep=';')
        print("Фактические колонки в CSV:", df.columns.tolist())
        
        # Обработка колонок с датами
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    # Применяем наш универсальный парсер дат
                    df[col] = df[col].apply(parse_custom_date)
                else:
                    raise ValueError(f"Колонка {col} не найдена в файле")
        
        # Особенность для таблицы проводок - полная перезапись
        if table_name == "DS.FT_POSTING_F":
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
        
        # Построчная загрузка данных
        rows_processed = 0
        for _, row in df.iterrows():
            try:
                # Для таблицы балансов
                if table_name == "DS.FT_BALANCE_F":
      