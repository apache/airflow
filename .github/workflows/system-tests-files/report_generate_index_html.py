import pandas as pd
import re
from jinja2 import Template
import tailer

pd.options.display.max_rows = 9999
fileName='reporttest.csv';
df = pd.read_csv(fileName, header=None)




count_t_s,count_t_f,count_tt_s,count_tt_f,count_sct_s,count_sct_f =0,0,0,0,0,0
array_t=''
array_tt=''
array_sct=''
result_t,result_tt,result_sct, time_t,time_tt,time_sct=[],[],[],0,0,0
for index, row in df.iterrows():
    chunks = re.split(' +', row[0])
    if chunks[0] == 'tests.system.providers.teradata.example_teradata':
        if chunks[1] == 'S':
            count_t_s+=1
            result_t.append('S')
        else:
            count_t_f+=1
            result_t.append('F')
        array_t=chunks
        time_t=chunks[2]
    elif chunks[0] == 'tests.system.providers.teradata.example_teradata_to_teradata_transfer':
        if chunks[1] == 'S':
            count_tt_s+=1
            result_tt.append('S')
        else:
            count_tt_f+=1
            result_tt.append('F')
        array_tt=chunks
        time_tt=chunks[2]
    else:
        if chunks[1] == 'S':
            count_sct_s+=1
            result_sct.append('S')
        else:
            count_sct_f+=1
            result_sct.append('F')
        array_sct=chunks
        time_sct=chunks[2]

last_t_10_elements = result_t[-10:]
last_tt_10_elements = result_tt[-10:]
last_sct_10_elements = result_sct[-10:]


items = []
an_item = dict(classname="tests.system.providers.teradata.example_teradata", successre=count_t_s, failurere=count_t_f, time=time_t, status=last_t_10_elements)
items.append(an_item)
an_item = dict(classname="tests.system.providers.teradata.example_teradata_to_teradata_transfer", successre=count_tt_s, failurere=count_tt_f,time=time_tt, status=last_tt_10_elements)
items.append(an_item)
an_item = dict(classname="tests.system.providers.teradata.example_ssl_teradata", successre=count_sct_s, failurere=count_sct_f, time=time_sct, status=last_sct_10_elements)
items.append(an_item)


# Create one external form_template html page and read it
File = open('report_index_template.html', 'r')
content = File.read()
File.close()

# Render the template and pass the variables
template = Template(content)
rendered_form = template.render(items=items)


# save the txt file in the index.html
output = open('index.html', 'w')
output.write(rendered_form)
output.close()
	