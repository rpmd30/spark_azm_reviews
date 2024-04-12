build:
	docker-compose build

build-nc:
	docker-compose build --no-cache

build-progress:
	docker-compose build --no-cache --progress=plain

down:
	docker-compose down --volumes --remove-orphans

run:
	make down && docker-compose up -d

run-scaled:
	make down && docker-compose up --scale spark-worker=3 -d

stop:
	docker-compose down

submit:
	docker exec da-spark-master spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3,com.mysql/mysql-connector-j:8.3.0 --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

clean:
	docker compose down && docker system prune && rm -r ./mysql_data/*

clean-all:
	docker compose down && docker system prune -all && rm -f ./mysql_data/*