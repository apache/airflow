from airflow.models import BaseOperator
from airflow.utils import send_MIME_email
from airflow.utils import apply_defaults
from airflow.hooks import PrestoHook
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
import logging
import jinja2
import os
import contrib.content_email_components as content_email_components
import subprocess

class ContentEmailOperator(BaseOperator):

    template_fields = ('data','subject','variables')
    template_ext = ('.sql')
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            to,
            subject,
            data,
            template,
            variables,
            presto_conn_id='presto_default',
            *args, **kwargs):
        super(ContentEmailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.subject = subject
        self.presto_conn_id = presto_conn_id
        self.data = data
        self.template = template
        self.variables = variables


    def formatData(self,df):
        output = []
        for i in range(len(df)):
            row = {}
            for col in list(df.columns):
                row[col] = df[col][i]
            output.append(row)
        return output

    def run(self,cmd):
        subprocess.Popen(cmd, shell=True).wait()

    def mkdir_local(self,name):
        self.run('mkdir {dir}'.format(dir=name))

    def rmdir_local(self,name):
        self.run('rm -R {dir}'.format(dir=name))

    def execute(self, context):
        #data
        data = {}
        hook = PrestoHook(presto_conn_id=self.presto_conn_id)
        for source, query in self.data.iteritems():
            data[source] = self.formatData(hook.get_pandas_df(query))

        #HTML template rendering
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.dirname(self.template)))
        env.globals['lib'] = content_email_components
        template = env.get_template(os.path.basename(self.template))
        cur_dir = '/tmp/'
        #self.mkdir_local(cur_dir + '/images')
        logging.info(os.path.dirname(os.path.realpath(__file__)))
        html = template.render({"data":data,"variables" :self.variables })

        #boilerplate email stuff
        strFrom = 'alerts@airbnb.com'
        strTo = ','.join(self.to)
        msgRoot = MIMEMultipart('related')
        msgRoot['Subject'] = self.subject
        msgRoot['From'] = strFrom
        msgRoot['To'] = strTo
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)
        msgText = MIMEText('This is the alternative plain text message.')
        msgAlternative.attach(msgText)
        msgText = MIMEText(html, 'html')
        msgAlternative.attach(msgText)

        # attach all images to email
        
        for file in os.listdir(cur_dir):
            if '.png' in file:
                fp = open(cur_dir + file, 'rb')
                msgImage = MIMEImage(fp.read())
                fp.close()
                msgImage.add_header('Content-ID', '<' + file[:-4] + '>')
                msgRoot.attach(msgImage)

        #send email
        send_MIME_email(strFrom,strTo,msgRoot)

        #clean up
        #self.rmdir_local('images')
        


