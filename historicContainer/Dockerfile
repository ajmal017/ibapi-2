# image base Python 
FROM python:3.6
# create /code and /data directories
RUN mkdir /code
RUN pip install pandas
WORKDIR /code
# copy local files to code directory 
COPY . /code/

# run the historical_request.py file
CMD python3 historical_request.py