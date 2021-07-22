FROM redis

RUN mkdir /opt/tutuka_app/

WORKDIR /opt/tutuka_app/

COPY . .

EXPOSE 6379

CMD ["redis-server"]