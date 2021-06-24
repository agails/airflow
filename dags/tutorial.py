#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial 
Documentação que acompanha o tutorial do Apache Airflow localizado 
[aqui](https://airflow.apache.org/tutorial.html)
"""
# [INICIO tutorial]
# [INICIO importação dos modulos]
from datetime import timedelta
from textwrap import dedent

# O objeto DAG; vamos precisar disso para instanciar um DAG
from airflow import DAG

# Operadores; precisamos disso para operar!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# [FIM importação dos modulos]

# [INICIO default_args]
# Esses argumentos serão repassados ​​para cada operador 
# Você cria uma base de argumentos que pode substituir 
# em cada tarefa durante a inicialização do operador
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# [FIM default_args]

# [INICIO instanciar_dag]
with DAG(
    'tutorial',
    default_args=default_args,
    description='Um tutorial simples DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['exemplo'],
) as dag:
    # [FIM instanciar_dag]

    # t1, t2 e t3 são exemplos de tarefas criadas por operadores de instanciação
    # [INICIO tarefa_básica]
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # [INICIO documentação]
    t1.doc_md = dedent(
        """\
    #### Documentação da tarefa 
    Você pode documentar sua tarefa usando os atributos `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` que é renderizado 
    na página de detalhes da instância da tarefa da IU. 
    ![Imgur](https://imgur.com/rmrtvO1.png)

    """
    )
    # desde que você tenha uma docstring no início do DAG
    dag.doc_md = __doc__ 
    # caso contrário, digite assim
    # dag.doc_md = """ Este tutorial mostra alguns dos conceitos fundamentais do Airflow, 
    #                  objetos e seu uso ao escrever seu primeiro pipeline.
    #                  Para saber mais visite 
    #                  [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)""" 
    # [FIM documentação]

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    # [FIM tarefa_básica]

    # [INICIO template_jinja]
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )
    # [FIM template_jinja]

    t1 >> [t2, t3]
# [FIM tutorial]