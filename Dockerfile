# Use the Apache Airflow 2.8.0 image as the base image 
FROM apache/airflow:2.8.0 

# Switch to the "airflow" user 
USER airflow 

# Install pip 
RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \ 
    python3 get-pip.py --user

# Install libraries from requirements.txt 
COPY requirements.txt /requirements.txt 
RUN pip install --user -r /requirements.txt 


