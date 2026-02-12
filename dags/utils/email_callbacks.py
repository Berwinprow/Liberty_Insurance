from airflow.utils.email import send_email
from airflow.models import TaskInstance


def _context_block(context):
    ti: TaskInstance = context["task_instance"]

    return f"""
    <br><b>DAG:</b> {context['dag'].dag_id}
    <br><b>Task:</b> {ti.task_id}
    <br><b>Run ID:</b> {context['run_id']}
    <br><b>Execution Date:</b> {context['execution_date']}
    <br><b>Try:</b> {ti.try_number} / {ti.max_tries}
    <br><b>Log URL:</b> <a href="{ti.log_url}">View Logs</a>
    """


def notify_failure(context):
    subject = f"❌ Airflow FAILED | {context['dag'].dag_id}"
    body = f"""
    <h3>Task Failed</h3>
    <b>Exception:</b><br>
    <pre>{context.get('exception')}</pre>
    {_context_block(context)}
    """

    send_email(
        to=["arunkumar.k@prowesstics.com"],
        subject=subject,
        html_content=body
    )


def notify_retry(context):
    subject = f"🔁 Airflow RETRY | {context['dag'].dag_id}"
    body = f"""
    <h3>Task Retrying</h3>
    {_context_block(context)}
    """

    send_email(
        to=["arunkumar.k@prowesstics.com"],
        subject=subject,
        html_content=body
    )


def notify_success(context):
    subject = f"✅ Airflow SUCCESS | {context['dag'].dag_id}"
    body = f"""
    <h3>DAG Completed Successfully</h3>
    {_context_block(context)}
    """

    send_email(
        to=["arunkumar.k@prowesstics.com"],
        subject=subject,
        html_content=body
    )
