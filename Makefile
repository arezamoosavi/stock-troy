ch:
	sudo chmod 777 -R data/

up:
	docker-compose up --build

down:
	docker-compose down -v