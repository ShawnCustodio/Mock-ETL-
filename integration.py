import sqlite3
import pandas as pd

def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Data extraction completed")
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()  

def transform_data(df):
    try:
        target_columns = ['id', 'name', 'date_of_birth', 'gender', 'medical_record', 'diagnosis', 'allergies', 'last_appointment_date']

        #Change Column Mapping Name when CSV is received to match with DB
        #Column names in DB: id, name, date_of_birth, gender, medical_record, diagnosis, allergies, last_appointment_date
        column_mapping = {
            'patient_id': 'id',
            'Name': 'name',
            'Gender': 'gender',
            'Medical Condition': 'medical_record',
            'Medication': 'diagnosis',
            'Discharge Date': 'last_appointment_date'
        }

        df.rename(columns={col: column_mapping.get(col, col) for col in df.columns}, inplace=True)

        df['id'] = range(1, len(df) + 1)

        # Ensure all target columns are present
        for column in target_columns:
            if column not in df.columns:
                if column in ['date_of_birth', 'last_appointment_date']:
                    df[column] = pd.NaT  
                else:
                    df[column] = 'Unknown'  

        df = df[target_columns]

        print("Data transformation completed")
        return df
    except Exception as e:
        print(f"Error transforming data: {e}")
        return df

def load_data(df, db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Append data to the existing table in the database
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print("Data loaded successfully")
        conn.close()
    except sqlite3.Error as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    file_path = 'healthcare_dataset.csv'  
    db_path = r'C:\Users\shawn\Medical.db'
    table_name = 'users'

    data = extract_data(file_path)
    transformed_data = transform_data(data)
    load_data(transformed_data, db_path, table_name)

    print("ETL Process Done")
