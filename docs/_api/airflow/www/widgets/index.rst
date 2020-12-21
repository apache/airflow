:mod:`airflow.www.widgets`
==========================

.. py:module:: airflow.www.widgets


Module Contents
---------------

.. py:class:: AirflowModelListWidget

   Bases: :class:`flask_appbuilder.widgets.RenderTemplateWidget`

   Airflow model list

   .. attribute:: template
      :annotation: = airflow/model_list.html

      


.. py:class:: AirflowDateTimePickerWidget

   Airflow date time picker widget

   .. attribute:: data_template
      :annotation: = <div class="input-group datetime datetimepicker"><span class="input-group-addon"><span class="material-icons cursor-hand">calendar_today</span></span><input class="form-control" %(text)s /></div>

      

   
   .. method:: __call__(self, field, **kwargs)




