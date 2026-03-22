# dedupe_deadline_alerts.py

sent_alerts = set()

def send_email(dag_id, email):
    if dag_id in sent_alerts:
        print(f"Skipping email for {dag_id}")
        return
    print(f"Sending email for {dag_id} to {email}")
    sent_alerts.add(dag_id)

# Example DAG alerts
send_email("dag_1", "abc@example.com")
send_email("dag_1", "abc@example.com")
send_email("dag_2", "xyz@example.com")
