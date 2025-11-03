# Airflow (Docker) Quickstart

## 0) Prérequis
- Docker Desktop installé et lancé
- Dans PowerShell, placez-vous à la racine de votre projet

## 1) Fichiers
- `docker-compose.yml`
- `.env` (avec AIRFLOW_UID)
Copiez ces fichiers à la racine du projet (même dossier que `dags/`, `scripts/`, `data/`).

## 2) Démarrage
```powershell
docker compose up airflow-init
docker compose up -d
```
Accédez à http://localhost:8080
- user: `airflow`
- pass: `airflow`

## 3) Déclarer la connexion AWS (UI)
Dans Airflow → **Admin > Connections** → **+** :
- Conn Id: `aws_default`
- Conn Type: `Amazon Web Services`
- Login: **Access key ID**
- Password: **Secret access key**
- Extra (JSON): `{"region_name":"eu-west-3"}`

## 4) Variables d'environnement pour le DAG (optionnel)
Vous pouvez définir (Admin > Variables) :
- `RAW_LOCAL`   -> `C:\\project_data\\data\\transactions_raw_20251028.csv`
- `CLEAN_LOCAL` -> `C:\\project_data\\data\\transactions_clean_local.csv`

Sinon, modifiez les chemins directement dans le DAG ou passez-les via des envs du service `webserver/scheduler`.

## 5) Lancer le DAG
- Activez le DAG `transactions_pipeline` dans l'UI (toggle ON)
- Cliquez sur **Play** pour un run manuel
- Vérifiez les logs des tâches

## 6) Où sont vos fichiers dans le conteneur ?
Le projet est monté dans `/opt/airflow`. Votre arbo devient :
```
/opt/airflow/
  dags/
  scripts/
  data/
  docs/
  requirements.txt
```

## 7) Arrêt
```powershell
docker compose down
```

## 8) Dépannage
- Si les providers Amazon ne sont pas trouvés, relancez `docker compose up -d` (installation via `_PIP_ADDITIONAL_REQUIREMENTS`).
- Pour voir les logs : `docker compose logs -f webserver`.
- Porte 8080 occupée ? Changez `"8080:8080"` dans docker-compose.