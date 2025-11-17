# Fichier docker-compose à utiliser pour l'infra Kafka
COMPOSE_FILE=./infrastructure/compose/kafka.compose.yml
COMPOSE=docker compose -f $(COMPOSE_FILE)

.PHONY: up down logs restart ps \
        up-% down-% logs-% restart-% \
        build build-% rebuild rebuild-%

# === Actions sur l'ensemble des conteneurs ===

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

# === Build des images ===

# Build de toutes les images définies dans le compose
build:
	$(COMPOSE) build

# Build d'un service spécifique
# Exemple : make build-kafka, make build-kafka-ui
build-%:
	$(COMPOSE) build $*

# Rebuild complet (sans cache) de toutes les images
rebuild:
	$(COMPOSE) build --no-cache

# Rebuild (sans cache) d'un service spécifique
# Exemple : make rebuild-kafka
rebuild-%:
	$(COMPOSE) build --no-cache $*

# === Actions sur un conteneur spécifique (service) ===
# Exemple : make up-kafka      -> démarre seulement le service "kafka"
#           make logs-kafka-ui -> suit les logs du service "kafka-ui"

up-%:
	$(COMPOSE) up -d $*

down-%:
	$(COMPOSE) stop $*

logs-%:
	$(COMPOSE) logs -f $*

restart-%:
	$(COMPOSE) restart $*
