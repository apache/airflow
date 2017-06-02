import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import os
import logging

def dataTable(data,dim_name, metric_name):
	#infer which range of dates is available for width of table
	date_list = []
	date_list_formatted = []
	for row in data:
		if row['ds'] in date_list:
			break
		else:
			date_list.append(row['ds'])
			if row['ds'][5] == '0':
				date_list_formatted.append(row['ds'][6:])
			else:
				date_list_formatted.append(row['ds'][5:])
	
	cnum = 0 #column number
	width = len(date_list)
	output = '''<table 
					border="1" 
					style="	border-collapse: collapse;
							border-top: none;
							font-family: Tahoma, Arial, Verdana, sans-serif;
							font-size:12px; " 
					cellpadding="3" 
					cellspacing="0" 
					width='775px'>'''
	date_options = '''<td align="center" style="font-style: italic;">'''
	date_headers = ''.join(map(lambda x: date_options + x + '</td>',date_list_formatted))
	output += '<tr bgcolor="#ccc"><td></td>' + date_headers + '</tr>' #first row
	for row in data:
		cnum += 1

		if int(row[metric_name]) == 0:
			value = '-'
			align = 'center'
		else:
			value = "{:,}".format(int(row[metric_name]))
			align = 'right'

		if cnum == 1:
			output += '<tr><td>' + row[dim_name] + '</td>' #dim name on first column
			output += '<td align="' + align + '">' + value + '</td>'
		elif cnum == width:
			output += '<td align="' + align + '"><b>' + value + '</b></td>' #bold last column
			cnum = 0 #reset counter
		else:
			output += '<td align="' + align + '">' + value + '</td>'
	output += '</table>' 
	return output

def line_graph(data,metric_name,chart_id,title):
	#infer which range of dates is available for width of table
	date_list = []
	for row in data:
		if row['ds'] in date_list:
			break
		else:
			date_list.append(row['ds'])
	rng = pd.date_range(date_list[0], periods=len(date_list), freq='D')
	logging.info(os.path.dirname(os.path.realpath(__file__)))
	img_dir = '/tmp/'
	pd.Series(map(lambda x: int(x[metric_name]),data), index=rng).plot(figsize=(8,3),fontsize=10,style=['k'])
	plt.title(title,size=10)
	plt.tight_layout()
	locs,labels = plt.yticks()
	plt.yticks(locs, map(lambda x: "{:,}".format(int(x)), locs))	
	plt.savefig(img_dir + chart_id + '.png')
	return '<img src="cid:{id}"/>'.format(id=chart_id)