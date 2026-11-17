# API AbAssurance

La documentation Swagger est disponible sur `/api/docs`, après connexion sur le site avec un compte administrateur.
Le fichier `/api/openapi.json` et la page `/api/redoc` demandent aussi ce rôle.
Les chemins ci-dessous sont relatifs à `/api`.

## Accès

Se connecter avec `POST /auth/login`. Le navigateur conserve le cookie de session.
Les requêtes POST doivent envoyer `X-Requested-With: AbAssurance`.
Les routes métier demandent un compte administrateur.
Un compte utilisateur peut consulter ses données avec `/me/data/{entity}` et se déconnecter.

## Routes

| Méthode | Chemin | Usage |
|---|---|---|
| POST | `/auth/login` | Se connecter avec username et password |
| GET | `/auth/me` | Voir le compte connecté |
| POST | `/auth/logout` | Fermer la session |
| GET | `/me/data/{entity}` | Consulter ses données personnelles |
| GET | `/data/clients` | Lire les clients d'une source |
| GET | `/data/contracts` | Lire les contrats d'une source |
| GET | `/data/claims` | Lire les sinistres d'une source |
| GET | `/data/payments` | Lire les paiements d'une source |
| GET | `/data/consolidated/{entity}` | Lire les données intégrées dans HDFS |
| GET | `/pipeline/services` | Vérifier les services |
| POST | `/pipeline/integrations` | Lancer un traitement |
| GET | `/pipeline/integrations/latest` | Suivre le dernier traitement |
| GET | `/health` | Vérifier si le modèle est chargé |
| GET | `/model` | Voir le modèle et ses métriques |
| POST | `/predict` | Prédire à partir de caractéristiques fournies |
| POST | `/predict/contract` | Prédire à partir d'un contrat consolidé |

Pour les lectures par source, ajouter `?source=oracle` ou `?source=sqlserver`.
Les valeurs de `entity` sont `clients`, `contracts`, `claims` et `payments`.
Les lectures consolidées et personnelles acceptent `offset` et `limit` (100 maximum).

## Prédiction

Le modèle doit être entraîné et disponible dans HDFS.
`GET /model` indique les variables utilisées et le statut du modèle.
`POST /predict` renvoie notamment `cancellation_probability`, `predicted_class` et `model_id`.
Un modèle expérimental n'est pas une validation métier du risque.

## Erreurs courantes

| Code | Signification |
|---|---|
| 401 | Session absente, expirée ou identifiants incorrects |
| 403 | Accès refusé ou en-tête requis manquant |
| 408 | Corps de requête reçu trop lentement |
| 413 | Corps de requête supérieur à 64 Kio |
| 404 | Données consolidées introuvables |
| 409 | Intégration déjà en attente ou en cours |
| 422 | Paramètres invalides |
| 429 | Trop de requêtes ou file Spark pleine ; attendre le délai Retry-After |
| 503 | Service ou modèle indisponible |
| 504 | Délai de traitement dépassé |
