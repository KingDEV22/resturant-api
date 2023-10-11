FROM python:3.11.4
WORKDIR /server
COPY ./requirements.txt /server/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /server/requirements.txt
COPY ./app /server/app
CMD ["uvicorn", "app.main:app"]
EXPOSE 8000