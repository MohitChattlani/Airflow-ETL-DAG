"""
### 
This ETL DAG is compatible with Airflow 1.10.x
"""
import csv
from textwrap import dedent
import requests
import json
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator


# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
}

# [START instantiate_dag]
with DAG(
    'final_etl_dag',
    default_args=default_args,
    description='ETL DAG Assignment',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
) as dag:

    # [START documentation]
    dag.doc_md = __doc__

    # [START extract_function]
    def extract(**kwargs):

        ti = kwargs['ti']

        #initialization
        totalPages=0
        final_resp_data=[]

        #first request
        resp1=requests.get('https://api.instantwebtools.net/v1/passenger?page=0&size=10')
        #200 status code check
        if (resp1.status_code==200):    
            resp1_text=resp1.text
            resp1_obj=json.loads(resp1_text)
            totalPages=resp1_obj["totalPages"]
            final_resp_data=resp1_obj["data"]

        #iterating through pages
        for i in range(1,totalPages):
            resp=requests.get(f'https://api.instantwebtools.net/v1/passenger?page={i}&size=10')
            if (resp.status_code==200):    
                resp_text=resp.text
                resp_obj=json.loads(resp_text)
                resp_data=resp_obj["data"]
                final_resp_data=final_resp_data+resp_data
        
        #checking data
        print(final_resp_data[:2])
        print(type(final_resp_data))        
        print(len(final_resp_data))

        ti.xcom_push('data_string', json.dumps(final_resp_data))    
                            
    # [START transform_function]
    def transform(**kwargs):
        ti = kwargs['ti']
        extract_data_string = ti.xcom_pull(task_ids='extract', key='data_string')
        resp_data = json.loads(extract_data_string)

        #resp_data_filtered=list(map(lambda obj:{'_id':obj['_id'],'name':obj['name'],'trips':obj['trips']},resp_data))
        
        resp_data_filtered=[]    
        for i in range(0,len(resp_data)):
            obj=resp_data[i]
            resp_data_obj={'_id':obj['_id'],'name':obj['name'],'trips':obj['trips']}
            resp_data_filtered.append(resp_data_obj)


        #checking data
        print(resp_data_filtered[:2])
        print(type(resp_data_filtered))        
        print(len(resp_data_filtered))
        
        ti.xcom_push('data_string', json.dumps(resp_data_filtered))

    # [START load_function]
    def load(**kwargs):
        ti = kwargs['ti']
        extract_data_string = ti.xcom_pull(task_ids='transform', key='data_string')
        resp_data = json.loads(extract_data_string)

        with open('passengers.csv', 'w', newline='') as file:
            fieldnames = ['_id','name','trips']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            for i in range(0,len(resp_data)):
                writer.writerow({'_id': resp_data[i]["_id"], 'name': resp_data[i]["name"],
                    'trips':resp_data[i]["trips"]})
        
                
    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data from an API.
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which filters only _id, name and trips columns 
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task and saves it in passengers.csv
    """
    )

    #dependency
    extract_task >> transform_task >> load_task

# [END main_flow]
