# Instructions copied from - https://hub.docker.com/_/python/
FROM python:3-onbuild
RUN pip install requests
RUN pip install grequests


# tell the port number the container should expose
EXPOSE 8080

# run the command
CMD ["python", "./api.py"]
