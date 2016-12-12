FROM ubuntu:latest
MAINTAINER Peter Georgeson "peter.georgeson@gmail.com"
# install base requirements
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install --upgrade pip

# copy app files
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

# run command
ENTRYPOINT ["python"]
CMD ["app.py"]
