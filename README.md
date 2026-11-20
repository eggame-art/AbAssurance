# AbAssurance

Unification des données des deux entreprises, AbAssurance (Oracle) et AssurePlus (SQL Server). Apache Hop extrait les clients, contrats, sinistres et paiements vers Kafka. Spark harmonise ces données et les stocke dans HDFS. L’API FastAPI et l’interface utilisateur permettent de consulter les données, de superviser les intégrations et d’estimer le risque de résiliation d’un contrat.

- [Application en local](http://localhost:5173)
- [Déploiement OVH](https://vps-8fb7c5b7.vps.ovh.net/)
- [Documentation de l’API](docs/API.md)
- [Exécutions GitHub Actions](https://github.com/eggame-art/AbAssurance/actions)

## Installation locale

### Prérequis

- Git et Docker avec le plugin Compose v2. Sous Windows, démarrer Docker Desktop en mode conteneurs Linux. Le service SQL Server utilise une image `linux/amd64`.
- Node.js 22.12 ou supérieur et npm pour l’interface.
- Python 3.11 recommandé pour les tests backend. Java 17 est nécessaire pour les tests Spark exécutés sur la machine. Docker fournit Python et Java pour les services conteneurisés.

### 1. Cloner et configurer le dépôt

```sh
git clone https://github.com/eggame-art/AbAssurance.git
cd AbAssurance
```

Sous PowerShell, créer `.env` à la racine lors de la première installation.

```powershell
Copy-Item .env.example .env
```

Sous macOS ou Linux, utiliser `cp .env.example .env`. Le modèle contient la configuration de démonstration pour une exécution locale. Garder le fichier `.env` hors du dépôt.

### 2. Démarrer les services et l’API

Exécuter ces commandes depuis la racine du dépôt. L’initialisation charge les schémas et un jeu de données fictives. Le premier démarrage peut prendre plusieurs minutes.

```sh
docker compose --profile hdfs up -d --wait oracle sqlserver kafka namenode datanode
docker compose run --rm --no-deps sqlserver-init
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs up -d --build --wait prediction-api
```

Vérifier que l’API répond sur [son endpoint de statut](http://127.0.0.1:8000/auth/status). Créer ensuite un compte administrateur local. La commande demande un mot de passe d’au moins 12 caractères.

```sh
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs exec prediction-api python -m abassurance.systems.auth.manage admin-local --role admin
```

Pour tester l’espace personnel, créer un compte rattaché au client Oracle `100` fourni par le jeu de démonstration.

```sh
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs exec prediction-api python -m abassurance.systems.auth.manage client-local --role user --source abassurance --source-id 100
```

### 3. Lancer l’interface

Dans un second terminal, depuis la racine du dépôt, exécuter les commandes suivantes. Sous PowerShell, utiliser `npm.cmd` si l’exécution de `npm.ps1` est bloquée.

```sh
cd client
npm ci
npm run dev
```

Ouvrir [l’application locale](http://localhost:5173) et se connecter avec `admin-local`. Dans **Pipeline de données**, sélectionner **Lancer l’intégration**. Une fois le traitement terminé, les données consolidées apparaissent dans **Données et prédictions**. Après connexion avec le compte administrateur, la [documentation Swagger](http://localhost:5173/api/docs) est accessible dans le même navigateur.

Vite redirige les appels `/api` vers `http://127.0.0.1:8000`. Si `API_PORT` est modifié dans `.env`, définir aussi `API_PROXY_TARGET` dans le terminal de Vite avec l’origine correspondante avant de lancer `npm run dev`.

### Entraîner un modèle de démonstration

La prédiction nécessite un modèle disponible dans HDFS. Pour en créer un sur un historique synthétique, ajouter ces chemins à `.env`, en remplaçant les valeurs déjà présentes pour `DATASET_OUTPUT_DIR` et `MODEL_OUTPUT_DIR`.

```dotenv
DATASET_OUTPUT_DIR=hdfs://namenode:9000/abassurance-synthetic/datasets/churn
MODEL_OUTPUT_DIR=hdfs://namenode:9000/abassurance-synthetic/models/churn
```

Avec les services locaux démarrés, exécuter depuis la racine du dépôt.

```sh
docker compose --profile hdfs run --rm --build spark-hdfs abassurance-history-demo --profile predictive --temporal-cohorts --history-dir hdfs://namenode:9000/abassurance-synthetic/history-predictive
docker compose --profile hdfs run --rm --build prepare-dataset abassurance-prepare-dataset --purpose synthetic-training --history-dir hdfs://namenode:9000/abassurance-synthetic/history-predictive --observation-dates 2025-07-01 2026-01-01 --evaluation-start 2026-01-01
docker compose --profile hdfs run --rm --build train-model abassurance-train-model --mode experimental
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs up -d --wait --force-recreate --no-deps prediction-api
```

Le score produit par ce modèle sert à la démonstration. Les conditions d’entraînement sur un historique réel sont décrites dans la [documentation de l’API](docs/API.md#prédiction).

### Consulter les journaux et arrêter les services

Depuis la racine, utiliser `ps -a` pour vérifier l’état des services et `logs` pour diagnostiquer un échec. Arrêter Vite avec `Ctrl+C`, puis arrêter les conteneurs avec `down`. Les données des volumes Docker sont conservées.

```sh
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs ps -a
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs logs --tail=100 prediction-api oracle sqlserver
docker compose -f compose.yaml -f compose.monitoring.yaml --profile api --profile hdfs down
```

## Tests et qualité du code

### Backend Python

Depuis la racine, créer un environnement virtuel et l’activer.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Sous macOS ou Linux, utiliser `python3.11 -m venv .venv` puis `source .venv/bin/activate`. Installer les dépendances de test et lancer les commandes suivantes dans l’environnement activé.

```sh
python -m pip install -e ".[dev,api,spark]"
python -m pytest -q
python scripts/test_coverage.py -q
python -m ruff check .
python -m ruff format --check .
```

Le rapport HTML se trouve dans `coverage/backend/html/index.html`. Pour inclure le test Spark d’isolation des données client comme dans la CI, utiliser `python scripts/test_coverage.py --with-customer-isolation -q` avec Java 17 et `SPARK_LOCAL_IP=127.0.0.1`.

### Frontend React

Depuis la racine du dépôt, dans un autre terminal si Vite tourne déjà.

```sh
cd client
npm ci
npm test
npm run test:coverage
npm run build
```

Le rapport HTML se trouve dans `client/coverage/index.html`. Le build vérifie TypeScript et génère `client/dist/`.

### Tests avec les services Docker

Depuis la racine, après le démarrage d’Oracle, de SQL Server et de Kafka et l’exécution de `sqlserver-init`, les tests suivants vérifient les connexions réelles, l’export et la publication Kafka. Installer le pilote Microsoft ODBC 18 sur la machine qui exécute les tests Python.

```sh
python -m pytest -q server/tests/integration/test_databases_pipeline.py --run-integration
python -m pytest -q server/tests/integration/test_kafka_roundtrip.py --run-kafka-integration
docker compose --profile hdfs run --rm --build spark-hdfs python -m pytest -q server/tests/spark/test_hdfs.py --run-hdfs-integration
```

Les tests de validation du déploiement suivants se lancent depuis la racine avec Node.js et Bash disponible dans le `PATH`.

```sh
node --test deploy/check-networks.test.mjs deploy/wait-sqlserver.test.mjs
```

## Déploiement

L’application est déployée sur un VPS OVH. Le domaine public est défini par `SITE_ADDRESS` dans `/opt/abassurance/.env` sur le serveur. L’URL publique reste à renseigner ici.

Le [workflow Docker CI and deployment](.github/workflows/docker-deploy.yml) exécute les tests, valide la configuration Docker et construit les images. Sur la branche principale du dépôt, il transfère ensuite les images et les sources vers le VPS via SSH. La configuration [Docker Compose de production](compose.production.yaml) lance les services et [Caddy](deploy/Caddyfile) sert le site et transmet les requêtes `/api` au backend. Les variables attendues figurent dans le [modèle de configuration de production](deploy/production.env.example).

## Structure du dépôt

| Dossier | Contenu |
| --- | --- |
| `client/` | Interface React, TypeScript, Vite et tests frontend |
| `server/src/abassurance/` | API FastAPI, intégration des données et prédiction |
| `server/tests/` | Tests backend, Spark et intégration |
| `Infrastructure/` | Schémas SQL, données de démonstration et configuration d’Apache Hop et Hadoop |
| `deploy/` | Configuration et scripts du déploiement OVH |

Les règles pour les branches et les messages de commit sont décrites dans le [guide de contribution](docs/BRANCHING.md).
