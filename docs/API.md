# API AbAssurance

La documentation Swagger est disponible sur `/api/docs`, après connexion sur le site avec un compte administrateur.
Le fichier `/api/openapi.json` et la page `/api/redoc` demandent aussi ce rôle.
Les chemins ci-dessous sont relatifs à `/api`.

## Accès

Se connecter avec `POST /auth/login`. Le navigateur conserve le cookie de session.
Les requêtes POST doivent envoyer `X-Requested-With: AbAssurance`.
Les routes d'administration demandent un compte administrateur.
Un compte utilisateur peut consulter ses données avec `/me/data/{entity}`, prédire sur ses contrats avec
`POST /me/predict/contract` et se déconnecter.

## Routes

| Méthode | Chemin | Usage |
|---|---|---|
| POST | `/auth/login` | Se connecter avec username et password |
| GET | `/auth/me` | Voir le compte connecté |
| POST | `/auth/logout` | Fermer la session |
| GET | `/me/data/{entity}` | Consulter ses données personnelles |
| POST | `/me/predict/contract` | Prédire sur un de ses contrats |
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

Le pourcentage affiché est le score calculé par le modèle, pas un taux de résiliation observé.
La sortie vient de `PipelineModel.transform(...).probability[1]` ; l'API ne tire pas de valeur au hasard.
Une valeur proche de 1 est affichée « Plus de 99,99 % » pour éviter de l'arrondir à 100 %.
Une sortie exactement égale à 1 reste affichée à 100 %, sans signifier une résiliation certaine.
Le détail du résultat montre le score brut et l'identifiant du modèle ; les données du calcul sont aussi consultables.
L'accès « Faire une prédiction » est disponible directement dans l'espace utilisateur.
En mode expérimental, le modèle est entraîné sur des données fictives et le résultat est une démonstration.
Les 10 000 clients fictifs ajoutés aux bases ne constituent pas un historique réel d'entraînement.

Pour entraîner sur des résultats réels, il faut l'historique des contrats, les résiliations avec leur date effective,
les sinistres et paiements datés, ainsi qu'une période de suivi complète pour les contrats non résiliés.
L'import existant `abassurance.systems.data_preparation.history` attend des fichiers Parquet unifiés
(`customers`, `contracts`, `claims`, `payments`, `cancellations`, `followup`).
La préparation sépare chronologiquement les données d'entraînement et d'évaluation.
L'entraînement réel utilise `--mode production` et refuse les données synthétiques ou sans cible fiable.
Aucun historique réel n'est actuellement fourni pour entraîner un nouveau modèle.

Pour un utilisateur, envoyer `contract_id`, `integration_id` et `observation_date` à `/me/predict/contract`.
Le client est déterminé par la session. Un contrat d'un autre client répond 404.
Les caractéristiques sont calculées par le serveur à partir de l'intégration choisie.
Le compte doit être associé à un client source. La documentation Swagger reste réservée aux administrateurs.

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
