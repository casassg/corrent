import json
from datetime import datetime, timedelta

from airflow.operators.email_operator import EmailOperator
from airflow.operators.http_operator import SimpleHttpOperator

from airflow import DAG

from corrent.decorators import operation
from corrent.core import inject
inject()

default_args = {
    'owner': 'Corrent',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    'send_server_ip', default_args=default_args, schedule_interval=None
) as dag:

  # Using default connection as it's set to httpbin.org by default
  get_ip = SimpleHttpOperator(
      task_id='get_ip', endpoint='get', method='GET', xcom_push=True
  )

  @operation
  def email_subject_generator(raw_json: str) -> str:
    external_ip = json.loads(raw_json)['origin']
    return f"Server connected from {external_ip}"

  @operation
  def email_body_generator(raw_json: str) -> str:
    return """
    Seems like today your server executing Airflow is connected from the external IP {origin}<br>
    <br>
    Have a nice day!
    """.format(**json.loads(raw_json))

  send_email = EmailOperator(
      task_id='send_email',
      to="example@example.com",
      subject='',
      html_content=''
  )

  ip_info = get_ip()
  subject = email_subject_generator(ip_info)
  body = email_body_generator(ip_info)
  send_email(subject=subject, html_content=body)