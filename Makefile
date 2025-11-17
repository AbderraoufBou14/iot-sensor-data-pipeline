# === Docker Compose Files ===
KAFKA_COMPOSE=./infrastructure/compose/kafka.compose.yml
SPARK_COMPOSE=./infrastructure/compose/spark.compose.yml

# Commandes Compose
KAFKA=docker compose -f $(KAFKA_COMPOSE)
SPARK=docker compose -f $(SPARK_COMPOSE)

.PHONY: \
    up down logs restart ps \
    up-all down-all logs-all restart-all build-all rebuild-all \
    kafka-up kafka-down kafka-logs kafka-restart kafka-ps \
    spark-up spark-down spark-logs spark-restart spark-ps \
    kafka-build kafka-build-% kafka-rebuild kafka-rebuild-% \
    spark-build spark-build-% spark-rebuild spark-rebuild-%


#  ACTIONS GLOBALES SUR KAFKA

kafka-up:
	$(KAFKA) up -d

kafka-down:
	$(KAFKA) down

kafka-logs:
	$(KAFKA) logs -f

kafka-restart:
	$(KAFKA) restart

kafka-ps:
	$(KAFKA) ps

kafka-build:
	$(KAFKA) build

kafka-build-%:
	$(KAFKA) build $*

kafka-rebuild:
	$(KAFKA) build --no-cache

kafka-rebuild-%:
	$(KAFKA) build --no-cache $*


# ACTIONS GLOBALES SUR SPARK


spark-up:
	$(SPARK) up -d

spark-down:
	$(SPARK) down

spark-logs:
	$(SPARK) logs -f

spark-restart:
	$(SPARK) restart

spark-ps:
	$(SPARK) ps

spark-build:
	$(SPARK) build

spark-build-%:
	$(SPARK) build $*

spark-rebuild:
	$(SPARK) build --no-cache

spark-rebuild-%:
	$(SPARK) build --no-cache $*

#COMMANDES "ALL" POUR TOUTES LES INFRASTRUCTURES

up-all:
	$(KAFKA) up -d
	$(SPARK) up -d

down-all:
	$(KAFKA) down
	$(SPARK) down

restart-all:
	$(KAFKA) restart
	$(SPARK) restart
