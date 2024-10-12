#Import necessary libraries
import os, shutil, glob
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
import pandas as pd
import logging



# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 8),  # Adjust to your desired start date
    'retries': 3,
    'retry_delay': timedelta(minutes= 5),
}

# Define the DAG
dag = DAG(
    'amazon_data_etl',
    default_args=default_args,
    description='An ETL process for Amazon data',
    schedule ='@daily',
)

# Directories from Airflow variables
directory = Variable.get("data_directory", default_var ="/opt/airflow/files/")
processed_dir = Variable.get("Processed_directory", default_var ="/opt/airflow/processed_files/")
transformed_dir = Variable.get("transformed_directory", default_var ="/opt/airflow/transformed_files/")
snowflake_conn_id = Variable.get("snowflake_conn_id", default_var= "snowflake")


# Extracting the data
def extract_data(**context):

    try:
        files = glob.glob(f"{directory}/*.csv")

        with open(os.path.join(processed_dir, 'processed_files.txt')) as f:
            processed_files = f.read().splitlines()

        processed_files_base = [os.path.basename(f) for f in processed_files]

        unprocessed_files = [f for f in files if os.path.basename(f) not in processed_files_base]

        if unprocessed_files:
            context['ti'].xcom_push(key = 'file_paths', value = unprocessed_files)
            print("data extracted successfully")
        else:
            logging.info("No new files to process")
            raise ValueError("No new files to process")
 
    # Exception if any error occurs
    except Exception as e:
        logging.error(f"Error during extracting:{str(e)}")
        raise


def transform_data(**context):
    

    try:
        #  Retrive all extracted filenames from XCom
        extracted_files = context['ti'].xcom_pull(task_ids= 'extract', key = 'file_paths')

        if not extracted_files:
            print("No files were extracted")
            return
        
        transformed_file_paths = []

        # Iterate through extracted filenames and transform each DataFrame
        for filename in extracted_files:
            df = pd.read_csv(filename)

            if df is None:
                logging.warning(f"No dataframe found")
                continue     # Skip to next iteration

            # Perform transformation
            logging.info(f"the description of data {df.describe()}")

            # Check for missing values
            missing_values = df.isnull()
            print(f"the total missing values: {missing_values.sum()}")

            # Print rows with missing values
            empty_rows = df[df.isnull().any(axis=1)]
            print(empty_rows)

            # Filter product names with missing values
            product_names = ['REDTECH USB-C to Lightning Cable 3.3FT, [Apple MFi Certified] Lightning to Type C Fast Charging Cord Compatible with iPhone 14/13/13 pro/Max/12/11/X/XS/XR/8, Supports Power Delivery - White',
                    'Amazon Brand - Solimo 65W Fast Charging Braided Type C to C Data Cable | Suitable For All Supported Mobile Phones (1 Meter, Black)']
            filtered_col = df.loc[df["product_name"].isin(product_names),'rating_count']
            print(filtered_col)

            # Drop rows with missing values as not suitable to fill the values
            df.dropna(inplace = True)

            # Print the total missing values again
            missing_values = df.isnull()
            print(missing_values.sum()) 

            # Remove duplicates if any
            duplicates = df.duplicated()
            print(df[duplicates])

            # Check the data types
            df.dtypes

            # Transform columns
            df['discounted_price'] = df['discounted_price'].str.replace('₹','').str.replace(',','')
            df['actual_price'] = df['actual_price'].str.replace('₹','').str.replace(',','')
            df['discount_percentage'] = df['discount_percentage'].astype(str)
            df['discount_percentage'] = df['discount_percentage'].str.replace('%', '')
            df['rating_count'] = df['rating_count'].str.replace(',','')
            df['rating'] = df['rating'].fillna('').astype(str).str.replace(r'[^0-9.]', '', regex=True)

            # Convert data types
            df['discounted_price'] = pd.to_numeric(df['discounted_price'])
            df['actual_price'] = pd.to_numeric(df['actual_price'])
            df['discount_percentage'] = pd.to_numeric(df['discount_percentage'])
            df['rating'] = pd.to_numeric(df['rating'])
            df['rating_count'] = pd.to_numeric(df['rating_count'])

            # Save the transformed data
            transformed_file_name = f"transformed_{os.path.splitext(os.path.basename(filename))[0]}.csv"
            transformed_file_path = os.path.join(transformed_dir, transformed_file_name)
            df.to_csv(transformed_file_path, index = False)
            print("Data transformed and saved successfully")
            print(transformed_file_path.split('/')[-1])

            transformed_file_paths.append(transformed_file_path)
            

            # Push the transformed file path to xcom
            context['ti'].xcom_push(key='transformed_file_path', value=transformed_file_path)
       
    except Exception as e:
        logging.error(f"Error while transforming the data: {e}")
        raise



def load_data(**context):
    try:
        transformed_file_path = context['ti'].xcom_pull(task_ids='transform', key='transformed_file_path')

        # Check if file path is none
        if transformed_file_path is None:
            print("NO transformed file path found")
            return    # Exit task early if no file path available
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id= snowflake_conn_id)
        
        # Upload file to Snowflake stage
        upload_sql = f"""
            PUT 'file://{transformed_file_path}' @AIRFLOW_STAGE AUTO_COMPRESS=TRUE;
        """
         
         # Load data from stage to table using COPY INTO
        copy_sql = f"""
            COPY INTO TRANSFORMED_ETL
            FROM @AIRFLOW_STAGE/{transformed_file_path.split('/')[-1]}
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER =1);
        """


        with snowflake_hook.get_conn() as conn:
            cursor = conn.cursor()

            try:
                conn.autocommit(False)      # Diable autocmmit
                # Execute upload to snowflake stage
                cursor.execute(upload_sql)
             
                # Execute data copy to snowflake table
                cursor.execute(copy_sql)

                conn.commit()    # Commit only if both succeed
                print("data loaded to table successfully")

                processed_file_path = os.path.join(processed_dir, 'processed_files.txt')
                # Update processed files
                with open(processed_file_path, 'a') as f:
                    f.write(f"transformed_{transformed_file_path.split('/')[-1]}\n")


            except Exception as e:
                print(f"Error during snowflake operation: {e}")
                conn.rollback()    # Rollback if error occurs

            finally:
                cursor.close()


    except Exception as e:
        print(f"Error loading data: {e}")


# Define the tasks in the DAG
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id = 'load',
    python_callable= load_data,
    dag = dag,
)
# Set task dependencies
extract_task >> transform_task >> load_task
