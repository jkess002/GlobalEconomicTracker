IMAGE_NAME=fetch-indices
ENV_FILE=.env

build:
	docker build -t $(IMAGE_NAME) scripts/output

run:
	docker run --rm --env-file $(ENV_FILE) $(IMAGE_NAME)

cron-line:
	@echo "0 17 * * 1-5 docker run --rm --env-file $(shell pwd)/$(ENV_FILE) $(IMAGE_NAME)"

clean:
	-docker rmi $(IMAGE_NAME)
