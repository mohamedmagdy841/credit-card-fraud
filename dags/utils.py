import requests
import pandas as pd
import sqlite3
import psycopg2

def extract(url):
    """
    Downloads a file from a specified URL and saves it locally.

    Args:
    url (str): The URL of the file to be downloaded.
    """
    try:
        r = requests.get(url)
        open('/opt/airflow/data/credit_card_fraud.csv', 'wb').write(r.content)
        print("File downloaded successfuly")
    except Exception as e:
        print(f"Error {e}")

def s3_upload(path, s3):
    """
    Uploads a file to an S3 bucket.

    Args:
    path (str): The local file path of the file to be uploaded.
    s3 (boto3.S3.Client): The S3 client object used for the upload.
    """
    try:
        s3.upload_file( 
            Filename=path, 
            Bucket="credit-card-fraud-bucket",
            Key="staging/credit_card_fraud.csv"
        )
    except Exception as e:
        print(f"Error uploading to s3: {e}")

def transform(path, s3):
    """
    Transforms the data from an S3 bucket and saves the transformed data locally.

    Args:
    path (str): The local file path where the transformed data will be saved.
    s3 (boto3.S3.Client): The S3 client object used to retrieve the data.
    """
    obj = s3.get_object(Bucket= "credit-card-fraud-bucket", Key= "staging/credit_card_fraud.csv")
    df = pd.read_csv(obj['Body'])

    # Drop unimportant columns
    df.drop(['Merchant Category Code (MCC)','CVV Code (Hashed or Encrypted)','Transaction Response Code',
        'Previous Transactions','User Account Information','Transaction Notes'
        ], axis=1, inplace=True)

    # Add "Rowid" column with auto-increment values
    df.insert(0, 'Rowid', range(1, len(df) + 1))

    # Check for missing values
    missing_values = df.isnull().sum()
    print("Missing values in each column:")
    print(missing_values)

    # Check for duplicates
    duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {duplicates}")

    # save data
    df.to_csv(path, index=False)

def move_to_transformed(path, s3):
    """
    Uploads the transformed file to an S3 bucket.

    Args:
    path (str): The local file path of the transformed file to be uploaded.
    s3 (boto3.S3.Client): The S3 client object used for the upload.
    """
    try:
        s3.upload_file( 
            Filename=path, 
            Bucket="credit-card-fraud-bucket",
            Key="transformed/transformed_credit_card_fraud.csv"
        )
    except Exception as e:
        print(f"Error uploading to s3: {e}")

def load(s3, hostname, user, pwd, port, database):
    """
    Loads the transformed data from an S3 bucket into a local PostgreSQL database.

    Args:
    s3 (boto3.S3.Client): The S3 client object used to retrieve the data.
    """
    obj = s3.get_object(Bucket= "credit-card-fraud-bucket", Key= "transformed/transformed_credit_card_fraud.csv")

    conn = psycopg2.connect(
        database=database, 
        user=user,
        password=pwd,
        host=hostname, 
        port= port
    )

    cursor = conn.cursor()


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS credit_card_fraud (
            Rowid INT PRIMARY KEY,
            `Transaction Date and Time` TEXT,
            `Transaction Amount` NUMERIC,
            `Cardholder Name` TEXT,
            `Card Number (Hashed or Encrypted)` TEXT,
            `Merchant Name` TEXT,
            `Transaction Location (City or ZIP Code)` TEXT,
            `Transaction Currency` TEXT,
            `Card Type` TEXT,
            `Card Expiration Date` TEXT,
            `Transaction ID` TEXT,
            `Fraud Flag or Label` INT,
            `Transaction Source` TEXT,
            `IP Address` TEXT,
            `Device Information` TEXT
        )
    """)

    print("Table created")

    df = pd.read_csv(obj['Body'])
    df.to_sql('credit_card_fraud', conn, if_exists= 'replace',index=False) 

    print("Data inserted")

    data = cursor.execute("""SELECT `Transaction Date and Time`,`Transaction Amount`,
                   `Cardholder Name`,`Card Type` FROM credit_card_fraud LIMIT 10;""")
    rows = cursor.fetchall()
    
    for column in data.description: 
        print(f"{column[0]}, ", end='') 

    print()

    for row in rows:
        print(row)
    conn.close()
