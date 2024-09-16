import sqlite3
import pandas as pd

def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Data extracted successfully")
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

def transform_data(df):
    try:
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['last_appointment_date'] = pd.to_datetime(df['last_appointment_date'], errors='coerce')

        df.fillna('Unknown', inplace=True)
        
        df.rename(columns={
            'medical_conditions': 'medical_record',
            'medications': 'diagnosis',
            'patient_id': 'id'
        }, inplace=True)
        
        for col in ['allergies', 'last_appointment_date', 'date_of_birth', 'gender']:
            if col not in df.columns:
                df[col] = 'Unknown' if col != 'date_of_birth' else pd.NaT

        print("Data transformed successfully")
        return df[['id', 'name', 'date_of_birth', 'gender', 'medical_record', 'diagnosis', 'allergies', 'last_appointment_date']]
    except Exception as e:
        print(f"Error transforming data: {e}")
        return df

def load_data(df, db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        
        def column_exists(cursor, table_name, column_name):
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            return column_name in columns
        
        with conn:
            cursor = conn.cursor()
            if not column_exists(cursor, table_name, 'allergies'):
                cursor.execute("ALTER TABLE users ADD COLUMN allergies TEXT;")
            if not column_exists(cursor, table_name, 'last_appointment_date'):
                cursor.execute("ALTER TABLE users ADD COLUMN last_appointment_date TEXT;")
            if not column_exists(cursor, table_name, 'date_of_birth'):
                cursor.execute("ALTER TABLE users ADD COLUMN date_of_birth TEXT;")
            if not column_exists(cursor, table_name, 'gender'):
                cursor.execute("ALTER TABLE users ADD COLUMN gender TEXT;")
        
        # Insert data into the database with conflict handling
        with conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        print("Data loaded successfully")
        conn.close()
    except sqlite3.Error as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    file_path = 'medical_records.csv'
    db_path = r'C:\Users\shawn\Medical.db'
    table_name = 'users'

    data = extract_data(file_path)
    transformed_data = transform_data(data)
    load_data(transformed_data, db_path, table_name)

    print("ETL Process Done")
