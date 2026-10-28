# Catalogue des données — projet fraude comportementale
_Généré automatiquement par `src/catalog.py` le 28/10/2026 à 12:47 (table `data_catalog`)._

## Tables
| zone    | table_name        |   lignes |   colonnes |
|:--------|:------------------|---------:|-----------:|
| raw     | raw_transactions  |    62600 |         14 |
| raw     | raw_fraud_labels  |      479 |          6 |
| raw     | raw_sessions      |    46692 |         13 |
| raw     | raw_merchants     |      320 |         14 |
| staging | stg_transactions  |    62350 |         11 |
| staging | stg_sessions      |    46692 |          9 |
| staging | stg_fraud_labels  |      479 |          3 |
| staging | stg_merchants     |      320 |          6 |
| staging | quarantine        |      250 |          4 |
| curated | ml_fraud_features |    62350 |         26 |

## Colonnes contenant des données personnelles et traitement appliqué
| zone    | table_name        | column_name     | categorie_rgpd    | traitement                                                                        |
|:--------|:------------------|:----------------|:------------------|:----------------------------------------------------------------------------------|
| raw     | raw_transactions  | customer_id     | INDIRECT          | en clair en RAW uniquement ; pseudonymisé (HMAC) en staging                       |
| raw     | raw_transactions  | device_id       | INDIRECT          | en clair en RAW uniquement ; pseudonymisé (HMAC) en staging                       |
| raw     | raw_transactions  | ip_address      | INDIRECT          | en clair en RAW uniquement ; tronquée /24 en staging ; absente en curated         |
| raw     | raw_sessions      | customer_id     | INDIRECT          | en clair en RAW uniquement ; pseudonymisé (HMAC) en staging                       |
| raw     | raw_sessions      | device_id       | INDIRECT          | en clair en RAW uniquement ; pseudonymisé (HMAC) en staging                       |
| raw     | raw_sessions      | ip_address      | INDIRECT          | en clair en RAW uniquement ; tronquée /24 en staging ; absente en curated         |
| raw     | raw_sessions      | latitude        | INDIRECT          | en clair en RAW uniquement ; remplacée par une zone en staging                    |
| raw     | raw_sessions      | longitude       | INDIRECT          | en clair en RAW uniquement ; remplacée par une zone en staging                    |
| raw     | raw_merchants     | name            | DIRECT (possible) | raison sociale : peut être un nom propre (entrepreneur individuel) ; non propagée |
| raw     | raw_merchants     | contact_email   | DIRECT (possible) | peut être nominatif ; non propagé                                                 |
| staging | stg_transactions  | customer_pseudo | PSEUDONYME        | HMAC-SHA256 + clé secrète, cohérent entre sources                                 |
| staging | stg_transactions  | device_pseudo   | PSEUDONYME        | HMAC-SHA256 + clé secrète, cohérent entre sources                                 |
| staging | stg_transactions  | ip_network      | INDIRECT          | réseau /24 (hôte supprimé) ; absent en curated                                    |
| staging | stg_sessions      | customer_pseudo | PSEUDONYME        | HMAC-SHA256 + clé secrète, cohérent entre sources                                 |
| staging | stg_sessions      | device_pseudo   | PSEUDONYME        | HMAC-SHA256 + clé secrète, cohérent entre sources                                 |
| staging | stg_sessions      | ip_network      | INDIRECT          | réseau /24 (hôte supprimé) ; absent en curated                                    |
| staging | stg_sessions      | zone_geo        | GÉNÉRALISÉE       | zone de service (10 villes) ; absente en curated                                  |
| curated | ml_fraud_features | customer_pseudo | PSEUDONYME        | HMAC-SHA256 + clé secrète, cohérent entre sources                                 |

Contrôle : la zone **curated** (`ml_fraud_features`) ne contient ni IP, ni coordonnées, ni identifiant en clair ; seule y figure la colonne pseudonymisée `customer_pseudo`.

## Détail par table
### raw_transactions (raw, conservation 90 jours)
Responsable : Direction des Opérations — base légale : Obligation légale (LCB-FT) + intérêt légitime (lutte contre la fraude)

| column_name    | dtype   |   null_pct |   distinct | categorie_rgpd   |
|:---------------|:--------|-----------:|-----------:|:-----------------|
| transaction_id | str     |        0   |      62600 | TECHNIQUE        |
| customer_id    | str     |        0   |       5000 | INDIRECT         |
| merchant_id    | str     |        0   |        320 | NONE             |
| amount         | float64 |        0.3 |      29357 | NONE             |
| currency       | str     |        0   |          2 | NONE             |
| channel        | str     |        0   |          4 | NONE             |
| status         | str     |        0   |          5 | NONE             |
| device_id      | str     |        0   |       4919 | INDIRECT         |
| ip_address     | str     |        0   |      62600 | INDIRECT         |
| created_at     | str     |        0   |      60952 | NONE             |
| updated_at     | str     |        0   |      60730 | NONE             |
| _source        | str     |        0   |          1 | TECHNIQUE        |
| _batch_id      | str     |        0   |          1 | TECHNIQUE        |
| _ingested_at   | str     |        0   |          1 | TECHNIQUE        |

### raw_fraud_labels (raw, conservation 90 jours)
Responsable : Direction des Risques — base légale : Intérêt légitime (lutte contre la fraude)

| column_name    | dtype   |   null_pct |   distinct | categorie_rgpd   |
|:---------------|:--------|-----------:|-----------:|:-----------------|
| transaction_id | str     |          0 |        479 | TECHNIQUE        |
| confirmed_at   | str     |          0 |        411 | NONE             |
| fraud_type     | str     |          0 |          4 | NONE             |
| _source        | str     |          0 |          1 | TECHNIQUE        |
| _batch_id      | str     |          0 |          1 | TECHNIQUE        |
| _ingested_at   | str     |          0 |          1 | TECHNIQUE        |

### raw_sessions (raw, conservation 90 jours)
Responsable : Direction Digitale (application mobile) — base légale : Intérêt légitime (sécurité des comptes et lutte contre la fraude)

| column_name   | dtype   |   null_pct |   distinct | categorie_rgpd   |
|:--------------|:--------|-----------:|-----------:|:-----------------|
| session_id    | str     |          0 |      46692 | TECHNIQUE        |
| customer_id   | str     |          0 |       5000 | INDIRECT         |
| device_id     | str     |          0 |       4437 | INDIRECT         |
| os            | str     |          0 |          3 | NONE             |
| app_version   | str     |          0 |          4 | NONE             |
| ip_address    | str     |          0 |      46691 | INDIRECT         |
| latitude      | float64 |          0 |      40369 | INDIRECT         |
| longitude     | float64 |          0 |      41149 | INDIRECT         |
| login_at      | str     |          0 |      46349 | NONE             |
| duration_s    | int64   |          0 |       1795 | NONE             |
| _source       | str     |          0 |          1 | TECHNIQUE        |
| _batch_id     | str     |          0 |          1 | TECHNIQUE        |
| _ingested_at  | str     |          0 |          1 | TECHNIQUE        |

### raw_merchants (raw, conservation 90 jours)
Responsable : Direction Partenariats — base légale : Exécution du contrat (partenaires)

| column_name   | dtype   |   null_pct |   distinct | categorie_rgpd    |
|:--------------|:--------|-----------:|-----------:|:------------------|
| merchant_id   | str     |          0 |        320 | NONE              |
| name          | str     |          0 |        288 | DIRECT (possible) |
| mcc           | str     |          0 |         10 | NONE              |
| category      | str     |          0 |         10 | NONE              |
| city          | str     |          0 |         10 | NONE              |
| country       | str     |          0 |          7 | NONE              |
| risk_level    | str     |          0 |          3 | NONE              |
| contact_email | str     |          0 |        320 | DIRECT (possible) |
| onboarded_at  | str     |          0 |        291 | NONE              |
| updated_at    | str     |          0 |        271 | NONE              |
| is_active     | bool    |          0 |          2 | NONE              |
| _source       | str     |          0 |          1 | TECHNIQUE         |
| _batch_id     | str     |          0 |          1 | TECHNIQUE         |
| _ingested_at  | str     |          0 |          1 | TECHNIQUE         |

### stg_transactions (staging, conservation 365 jours)
Responsable : Direction des Opérations — base légale : Obligation légale (LCB-FT) + intérêt légitime (lutte contre la fraude)

| column_name     | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:----------------|:---------------|-----------:|-----------:|:-----------------|
| transaction_id  | str            |          0 |      62350 | TECHNIQUE        |
| customer_pseudo | str            |          0 |       5000 | PSEUDONYME       |
| device_pseudo   | str            |          0 |       4914 | PSEUDONYME       |
| merchant_id     | str            |          0 |        320 | NONE             |
| amount          | float64        |          0 |      29356 | NONE             |
| currency        | str            |          0 |          1 | NONE             |
| channel         | str            |          0 |          4 | NONE             |
| status          | str            |          0 |          4 | NONE             |
| ip_network      | str            |          0 |      62142 | INDIRECT         |
| created_at      | datetime64[us] |          0 |      60713 | NONE             |
| updated_at      | datetime64[us] |          0 |      60492 | NONE             |

### stg_sessions (staging, conservation 365 jours)
Responsable : Direction Digitale (application mobile) — base légale : Intérêt légitime (sécurité des comptes et lutte contre la fraude)

| column_name     | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:----------------|:---------------|-----------:|-----------:|:-----------------|
| session_id      | str            |          0 |      46692 | TECHNIQUE        |
| customer_pseudo | str            |          0 |       5000 | PSEUDONYME       |
| device_pseudo   | str            |          0 |       4437 | PSEUDONYME       |
| os              | str            |          0 |          3 | NONE             |
| app_version     | str            |          0 |          4 | NONE             |
| ip_network      | str            |          0 |      46625 | INDIRECT         |
| zone_geo        | str            |          0 |         10 | GÉNÉRALISÉE      |
| login_at        | datetime64[us] |          0 |      46349 | NONE             |
| duration_s      | int64          |          0 |       1795 | NONE             |

### stg_fraud_labels (staging, conservation 365 jours)
Responsable : Direction des Risques — base légale : Intérêt légitime (lutte contre la fraude)

| column_name    | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:---------------|:---------------|-----------:|-----------:|:-----------------|
| transaction_id | str            |          0 |        479 | TECHNIQUE        |
| fraud_type     | str            |          0 |          4 | NONE             |
| confirmed_at   | datetime64[us] |          0 |        411 | NONE             |

### stg_merchants (staging, conservation 365 jours)
Responsable : Direction Partenariats — base légale : Exécution du contrat (partenaires)

| column_name   | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:--------------|:---------------|-----------:|-----------:|:-----------------|
| merchant_id   | str            |          0 |        320 | NONE             |
| mcc           | str            |          0 |         10 | NONE             |
| category      | str            |          0 |         10 | NONE             |
| country       | str            |          0 |          7 | NONE             |
| risk_level    | str            |          0 |          3 | NONE             |
| updated_at    | datetime64[us] |          0 |        271 | NONE             |

### quarantine (staging, conservation 365 jours)
Responsable : Direction des Risques — base légale : Intérêt légitime (lutte contre la fraude)

| column_name    | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:---------------|:---------------|-----------:|-----------:|:-----------------|
| table_name     | str            |          0 |          1 | NONE             |
| record_id      | str            |          0 |        250 | NONE             |
| reason         | str            |          0 |          2 | NONE             |
| quarantined_at | datetime64[us] |          0 |          2 | NONE             |

### ml_fraud_features (curated, conservation 1825 jours)
Responsable : Direction des Risques — base légale : Intérêt légitime (lutte contre la fraude)

| column_name                        | dtype          |   null_pct |   distinct | categorie_rgpd   |
|:-----------------------------------|:---------------|-----------:|-----------:|:-----------------|
| transaction_id                     | str            |       0    |      62350 | TECHNIQUE        |
| customer_pseudo                    | str            |       0    |       5000 | PSEUDONYME       |
| created_at                         | datetime64[us] |       0    |      60713 | NONE             |
| montant_log                        | float64        |       0    |      29356 | NONE             |
| heure                              | int64          |       0    |         24 | NONE             |
| est_nuit                           | int64          |       0    |          2 | NONE             |
| canal_app                          | int64          |       0    |          2 | NONE             |
| canal_ussd                         | int64          |       0    |          2 | NONE             |
| canal_card                         | int64          |       0    |          2 | NONE             |
| canal_web                          | int64          |       0    |          2 | NONE             |
| velocite_1h                        | int64          |       0    |          6 | NONE             |
| velocite_24h                       | int64          |       0    |          6 | NONE             |
| risque_marchand                    | int64          |       0    |          3 | NONE             |
| nb_sessions_24h_avant              | int64          |       0    |          5 | NONE             |
| minutes_depuis_derniere_session    | float64        |       0    |      16517 | NONE             |
| duree_derniere_session_s           | int64          |       0    |       1796 | NONE             |
| a_historique_session               | int64          |       0    |          2 | NONE             |
| appareil_deja_vu_session           | int64          |       0    |          2 | NONE             |
| appareil_connu_plus_24h            | int64          |       0    |          2 | NONE             |
| nb_appareils_distincts_7j          | int64          |       0    |          4 | NONE             |
| nb_zones_distinctes_24h            | int64          |       0    |          4 | NONE             |
| zone_derniere_session_inhabituelle | int64          |       0    |          2 | NONE             |
| is_fraud                           | int64          |       0    |          2 | NONE             |
| fraud_type                         | str            |      99.23 |          4 | NONE             |
| fraud_confirmed_at                 | datetime64[us] |      99.23 |        411 | NONE             |
| _built_at                          | datetime64[us] |       0    |          1 | TECHNIQUE        |
