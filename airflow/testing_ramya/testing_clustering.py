import numpy as np
import pandas as pd
import requests
import mailer
import psycopg2 as pg
import datetime
import requests


# In[13]:


from airflow import DAG


# In[14]:


conn = pg.connect(host='healthiviti.cl63skfmm5ia.ap-south-1.redshift.amazonaws.com', port=5439, database="skull", user="ramya_koteswari", password='qAkhNgtMBJEwDjwweqpg3yrg')


# In[26]:


q="""select * from healthiviti_mdm_mdm.item_distributor_mapping limit 10"""


# In[27]:


df1=pd.read_sql(q, conn)


# In[29]:


if len(df1)>0:
    subject = "List of items from clusters sent for NPC" + str((datetime.datetime.now()).strftime('%Y_%m_%d')) 

    html_greetings = '''
    <p>Dear User,</p><p>\PFA items list for NPC.</p>
    '''
    html_body = '<html><body>'+html_greetings+ '<p><b> Summary: </b><br>'+'<br>'+'<p><b>Utilisation- </b></p> Total Man Hrs Worked / 8(hrs) <br>'+'<br><p> <b>Efficiency- </b> </p>'+' Target TAT / Daily Avg TAT '+'<br><p> Target TAT for Maker is 2.7 mins/task,Checker is 0.8 mins/task, Mapper is 0.5mins/task, Validator is 0.25mins/task </p>'+'<a href="https://tableau.ahwspl.net/#/views/MappingQualityDashboard_0/UserSummary?:iid=1"><b> Tableau Dashboard Link</b> </a>' +'<p><br><br>Warm Regards, <br> Team Healthiviti</p>'+'</body></html>'
    html_body += '<img src="cid:image1">'
    
    mail = mailer.Email('smtp.gmail.com', 587)
#     mail.attach(df_csv)
#     mail.attach(dffile1)
    mail.set_from('notifications@healthiviti.com')
    mail.set_to(['ramya.koteswari@healthiviti.com','esther.amulya@healthiviti.com'])   # every monday get email ids?
    mail.set_subject(subject)
    mail.set_mssg_html(html_body)
    mail.send('notify@1234')
