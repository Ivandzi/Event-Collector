# requirements
Go 1.23+
PostgreSQL 16+
RabbitMQ 3+

# launch
docker compose up --build

# docker
docker compose ps

![alt text](screen1.png)

# logs
docker compose logs server

![alt text](screen2.png)

# http api
for ($i = 1; $i -le 10; $i++) {
    $body = "{`"user_id`":$i,`"event_type`":`"test`",`"timestamp`":`"2026-10-16T12:00:00Z`"}"
    Invoke-RestMethod -Uri "http://localhost:8080/reception" -Method Post -ContentType "application/json" -Body $body
}

# swagger ui
http://localhost:8080/swagger/index.html

![alt text](screen3.png)

http://localhost:8080/swagger/doc.json

![alt text](screen4.png)

# rabbitmq
http://localhost:15672

![alt text](screen5.png)

![alt text](screen6.png)

# postgresql
docker compose exec db psql -U postgres -d events -c "\dt"

![alt text](screen7.png)

docker compose exec db psql -U postgres -d events -c "SELECT * FROM events;"

![alt text](screen8.png)

# completion
docker compose down