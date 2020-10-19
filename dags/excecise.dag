import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2020, 10, 8, 10, 00, 00),
    'retries': 1,
}


def get_results(sql, file_name, comments):
    request = sql
    pg_hook = PostgresHook(postgre_conn_id="postgres", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    file = open(file_name, 'w')
    file.write(str(comments) + '\n')
    for res in sources:
        file.write(str(res) + '\n')
    file.close()
    cursor.close()


with DAG('exercise_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')

    create_task = BashOperator(
        task_id="create_task",
        bash_command="psql -h localhost -p 5432 -U airflow -d postgres -f /data/init.sql",
        dag=dag,
    )

    waiting_docker_up = BashOperator(
        task_id='waiting_docker_up',
        bash_command='sleep 50',
        dag=dag,
    )

    insert1_task = BashOperator(
        task_id="insert_task",
        bash_command="psql -h localhost -p 5432 -U airflow -c  "copy users FROM '/data/dataset_users.csv' delimiter '|' csv",
        dag=dag,
    )

    insert2_task = BashOperator(
        task_id="insert_task",
        bash_command="psql -h localhost -p 5432 -U airflow -c  "copy users FROM '/data/dataset_user_interaction.csv' delimiter '|' csv",
        dag=dag,
    )

    sleep_task = BashOperator(
        task_id='sleep_for_eleven_minutes',
        bash_command='sleep 50',
        dag=dag,
    )

    select_1_task = PythonOperator(
        task_id='select_1_task',
        python_callable=get_results,
        op_kwargs={
            "sql": "select user_id, event_id, event_type, event_time
            from user_interaction where user_id not in (select user_id from users)",
            "file_name": "answer1.txt",
            "comments": "Business Questions. Quest 1. Events with wrong user_id which does not even exist in our database "},
        dag=dag,
    )

    select_2_task = PythonOperator(
        task_id='select_2_task',
        python_callable=get_results,
        op_kwargs={
            "sql": "select DISTINCT event_time from user_interaction where event_id in
            (select event_id from (select  event_id, count(*)
                                   from user_interaction
                                   group by user_id, event_id
                                   HAVING count(*) > 3) as ps)",
            "file_name": "answer2.txt",
            "comments": "Business Questions. Quest 2. Lot of events duplicated on one specific day. Which day was that?"},
        dag=dag,
    )
    select_3_task = PythonOperator(
        task_id='select_3_task',
        python_callable=get_results,
        op_kwargs={
            "sql": "
            select user_id, country from users
            where user_id in
            (select  user_id from
             (select user_id, count(event_type) as counter from user_interaction where event_type='VIEW' group by
              user_id having count(*) >= 1) as country
             where counter >= (select  max(counter) from
                               (select user_id, count(event_type) as counter
                                from user_interaction
                                where event_type='VIEW' group by  user_id having count(*) >= 1) as country
                               ))",
            "file_name": "answer3.txt",
            "comments": "Business Questions. Quest 3.From which country did the users visit our website the most"},
        dag=dag,
    )
    select_4_task = PythonOperator(
        task_id='select_4_task',
        python_callable=get_results,
        op_kwargs={
            "sql": "select age from users where
            user_id in
            (select user_id from user_interaction where event_type='VIEW'
             group by user_id having count(event_type) >= 5
             INTERSECT
             select user_id from user_interaction where event_type='CLICK'
             group by user_id having count(event_type) >= 10)",
            "file_name": "answer4.txt",
            "comments": "the most valuable users (a user with a minimum 5 VIEWs and 10 CLICKs). Are these users coming from the younger (18-49) or the elder (50-80) generation"},
        dag=dag,
    )


start_task >> create_task >> waiting_docker_up >> insert1_task >> insert2_task >> sleep_task
sleep_task >> select_1_task >> select_2_task
select_2_task >> select_3_task >> select_4_task
