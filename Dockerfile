FROM python:latest

RUN pip install mysql-connector
RUN pip install plotly
RUN pip install plotly --upgrade
RUN pip install scipy
RUN pip install numpy
RUN pip install python-dotenv
WORKDIR /script
ENTRYPOINT ["python3", "Script.py"]