
"""
## Projeto - Relatório de Vendas  
Temos um cliente de varejo que possui vários pontos de venda localizados em diferentes localidades. 
A partir dessas transações que acontecem nesssas várias lojas, a empresa deseja obter um 
"relatório de lucro diário" com o desempenho de suas lojas no mercado. 
Como entrada, recebemos diariamente um arquivo CSV bruto contendo os dados das transações 
de todas as lojas.
"""

from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.p import MySqlOperator
from airflow.operators.email import EmailOperator

from datacleaner import data_cleaner

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Agail Sanchez',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval': timedelta(days=1)
    }
    
with DAG(
    'Sales_Report_dag',
    default_args=default_args,
    description='Simple ETL to generate a sales report',
    schedule_interval="0 4 * * *",
    template_searchpath=['/usr/local/airflow/data/sql_files'],
    catchup=True,
    tags=['Projeto']) as dag:

    dag.doc_md = __doc__

    t1=BashOperator(
        task_id='check_file_exists',
        bash_command='shasum /usr/local/airflow/data/store_files/raw_store_transactions.csv',
        retries=2,
        retry_delay=timedelta(seconds=15)
        )
    
    t1.doc_md = dedent(
        """
        ### Documentação da tarefa
        Tarefa que verifica se o arquivo de entrada está na pasta
        """
        )
    t2 = PythonOperator(
        task_id='clean_raw_csv',
        python_callable=data_cleaner
        )
    
    t2.doc_md = dedent(
        """
        ### Documentação da tarefa
        Tarefa que limpa os dados do arquivo de entrada
        """
        )

    # t3 = MySqlOperator(task_id='create_mysql_table', mysql_conn_id="mysql_conn", sql="create_table.sql")

    # t4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id="mysql_conn", sql="insert_into_table.sql")

    # t5 = MySqlOperator(task_id='select_from_table', mysql_conn_id="mysql_conn", sql="select_from_table.sql")

    # t6 = BashOperator(task_id='move_file1', bash_command='cat ~/store_files_airflow/location_wise_profit.csv && mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date)

    # t7 = BashOperator(task_id='move_file2', bash_command='cat ~/store_files_airflow/store_wise_profit.csv && mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date)

    # t8 = EmailOperator(task_id='send_email',
    #     to='example@example.com',
    #     subject='Daily report generated',
    #     html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
    #     files=['/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date, '/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date])

    # t9 = BashOperator(task_id='rename_raw', bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' % yesterday_date)

    # t1 >> t2 >> t3 >> t4 >> t5 >> [t6,t7] >> t8 >> t9
