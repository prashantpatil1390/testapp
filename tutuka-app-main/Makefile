#redis-cli lpush transactions "$(cat sample_transaction.json | tr '\r\n' ' ')"
#redis-cli lrange transactions 0 2

delete:
	docker rm -f tutuka-app-mohansha

build:
	docker build -t mohansha/tutuka-app .
	docker push mohansha/tutuka-app

run:
	docker run -d -p 127.0.0.1:6379:6379 --name tutuka-app-mohansha mohansha/tutuka-app
	docker exec -it	tutuka-app-mohansha /bin/bash
