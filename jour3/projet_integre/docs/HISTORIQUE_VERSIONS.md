# Historique des versions du jeu de données `ml_fraud_features`

Chaque version = un **tag Git** (code + paramètres + pointeur DVC) + un **hash DVC** (contenu exact des données).
Les données sont dans le cache `.dvc/cache` et le stockage distant `~/dvc_remote_nexapay` ; Git ne contient que le
pointeur `data/ml_fraud_features.dvc`.

| Version | Hash DVC | Lignes | Fraudes | Taux | Dernière transaction | Empreinte contenu | Changement |
|---|---|---|---|---|---|---|---|
| `data-v1` | `7074435e28a5aef0750a19f0cbcf1024` | 60 358 | 458 | 0,76 % | 2026-10-28 11:19 | `eac5d42532f7843b` | Première version : 9 features de session, historique depuis le 28/07 |
| `data-v2` | `65cd25cdf911c88d92df4df370a5fcdd` | 62 350 | 479 | 0,77 % | 2026-10-28 11:40 | `19fb35b1fa155fbe` | +2 000 transactions et +976 sessions (activité simulée), collecte incrémentale |

Les deux versions ont passé le contrôle de sortie bloquant avant d'être versionnées.

## Comment les versions ont été produites

```bash
# v1 — après un run complet réussi du DAG
bash jour3/projet_integre/scripts/versionner.sh data-v1 "Features v1 : 9 features de session, historique au 28/10"

# v2 — nouvelle activité, DAG relancé (collecte incrémentale), contrôle de sortie au vert
python -m fil_rouge.generate_data --append 2000
airflow dags test projet_fraude_comportementale
bash jour3/projet_integre/scripts/versionner.sh data-v2 "Features v2 : +2000 transactions et nouvelles sessions"
```

## Comparer et revenir à une version

```bash
dvc diff data-v1 data-v2                        # fichiers modifiés entre versions
git show data-v1:jour3/projet_integre/data/ml_fraud_features.dvc
git checkout data-v1 && dvc checkout            # données ET code exacts de la v1
cat jour3/projet_integre/data/ml_fraud_features/MANIFEST.json
git checkout main && dvc checkout
```

Vérifié : retour à la v1 → `eac5d42532f7843b` (60 358 lignes) ; retour à la v2 → `19fb35b1fa155fbe` ;
suppression de `.dvc/cache` et des données puis `dvc pull` → `19fb35b1fa155fbe` à l'identique.

## Pourquoi l'instantané est déterministe

`export_snapshot.py` trie par `transaction_id` et retire `_built_at` (horodatage du calcul) : relancer le pipeline sans
nouvelle donnée redonne la même empreinte de contenu (vérifié : deux runs Airflow successifs → `eac5d42532f7843b`),
donc DVC ne crée pas de fausse nouvelle version.

## Note sur l'historique Git

Le contexte Great Expectations (`gx/`) avait été commité par erreur avec le code : il est régénéré à chaque exécution
et contenait les listes d'identifiants utilisées par les contrôles d'intégrité. Il a été retiré de tout l'historique
(dépôt local, jamais partagé) et ajouté au `.gitignore` ; les hash DVC n'ont pas changé.
