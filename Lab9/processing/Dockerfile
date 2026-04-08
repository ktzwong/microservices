FROM python:3.12

LABEL maintainer="your-email@bcit.ca"

RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8100

ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]
