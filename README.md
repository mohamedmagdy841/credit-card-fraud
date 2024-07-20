# ETL Pipeline for Credit Card Fraud Detection Using Airflow

### Description
This project implements an automated ETL pipeline for credit card fraud detection using Python and Apache Airflow. It extracts data from S3, cleans and transforms it, and loads it into PostgreSQL for analysis. Apache Airflow manages the workflow, and AWS S3 provides secure, scalable storage.


### Dataset Summary:  
The `credit_card_fraud` dataset consists of 20 diverse metrics and 8000 transaction records.

**Fields include**:
- Transaction details (Date and Time, Amount, ID)
- Cardholder and card details (Name, Encrypted Card Number, Type, Expiration Date, Encrypted CVV)
- Merchant details (Name, Category Code)
- Location details (City or ZIP Code, Currency)
- Fraud detection metrics (Fraud Flag, Previous Transactions)
- Transaction source details (Source, IP Address, Device Information)
- Additional details (Response Code, User Account Information, Transaction Notes)

### Architecture:

<p align="center">
  <img width="700" src="https://github.com/user-attachments/assets/427c3671-dec2-48d2-b0a8-4867922c6009">
</p>

### Project Overview

1. **Extract Data**
   - Extract raw transaction data from a CSV file and store it in the staging area of an S3 bucket.

2. **Transform Data**
   - Clean and preprocess the data, including dropping unimportant columns, adding new identifiers, and checking for missing values and duplicates.
   - Save the cleaned data into a "transformed" folder in the S3 bucket.

3. **Load Data**
   - Load the transformed data from S3 into an PostgreSQL database for further analysis.

4. **Analyze Data**
   - Use SQL queries on the PostgreSQL database to perform analysis and derive insights.

## S3 Bucket
<p align="center">
  <img width="800" src="https://github.com/user-attachments/assets/2026fa5f-96df-489b-9014-36b8ba5c2757">
</p>

## Graph
<p align="center">
  <img width="800" src="https://github.com/user-attachments/assets/5c1c992d-d870-4528-9cf2-b0a632046f46">
</p>

## Query Logs
<p align="center">
  <img width="800" src="https://github.com/user-attachments/assets/fbb2e0ee-2bd3-4453-a075-cc981c267940">
</p>
