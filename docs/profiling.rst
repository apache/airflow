Data Profiling
==============

Part of being a productive with data is about having the right weapons to
profile the data you are working with. Airflow provides a simple query 
interface to write sql and get results quickly, and a charting application 
letting you visualize data.

Adhoc Queries
-------------
The adhoc query UI allows for simple SQL interaction with the database
connections registered in Airflow.

.. image:: img/adhoc.png

Charts
-------------
A simple UI built on top of flask-admin and highcharts allows to build
data visualizations and charts easily. Fill in a form with a label, sql,
chart type, pick a source database from your environment's connecton, 
select a few other options, and save it for later use. 

You can even use the same templating and macros available when writing 
airflow pipelines, parameterizing your queries and modifying parameters 
directly in the URL.

These charts are basic, but they're easy to create, modify and share.

Chart Screenshot
................

.. image:: img/chart.png

-----

Chart Form Screenshot
.....................

.. image:: img/chart_form.png
