FROM apache/airflow:2.5.0-python3.9
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user awscli
# RUN pip install --user snowflake-connector-python snowflake-sqlalchemy apache-airflow-providers-snowflake
RUN pip install --no-cache-dir --user -r /requirements.txt

# RUN cd /tmp 
# RUN python -m venv dbt_venv  

# RUN source dbt_venv/bin/activate  
# RUN pip install --no-cache-dir dbt-snowflake
# RUN deactivate

# COPY --from=builder /dbt_venv /dbt_venv
# ENV PATH=/dbt_venv/bin:$PATH
