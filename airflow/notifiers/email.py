from airflow.notifiers import AirflowNotifier
from airflow.utils.email import send_email
    
    
class EmailNotifier(AirflowNotifier):
    
    def __init__(self, email, notify_on = {}):
        self.email = email
        self.notify_on = notify_on
    
    def send_notification(self, task_instance, state, message, **kwargs):
        if state in self.notify_on:
            title_tpl, body_tpl = self.notify_on[state]
            
            title = self.render_template(title_tpl, 
                                         task_instance,
                                         message=message)
            body = self.render_template(body_tpl,
                                        task_instance, 
                                        message=message, 
                                        **kwargs)
            
            send_email(self.email, title, body)
        