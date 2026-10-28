# Fiche de registre des traitements (RGPD art. 30)

**Traitement** : Construction des features de détection de fraude à partir des transactions et des sessions de
l'application mobile NexaPay.

| Rubrique (art. 30.1) | Contenu |
|---|---|
| **Responsable du traitement** | NexaPay (fintech de paiement mobile, zone UEMOA) — Direction des Risques |
| **Délégué à la protection des données** | DPO NexaPay (contact : dpo@nexapay.example — fictif) |
| **Finalité** | Détection et prévention de la fraude au paiement : alimentation d'un modèle de scoring à partir du comportement dans l'application (sessions, appareils, zones de connexion) |
| **Base légale** | Intérêt légitime de NexaPay et de ses clients à lutter contre la fraude (art. 6.1.f) ; obligations légales LCB-FT pour les transactions (art. 6.1.c) |
| **Personnes concernées** | Clients particuliers et professionnels utilisant l'application mobile NexaPay |
| **Catégories de données** | Identifiants internes (client, appareil, session, transaction) ; données de transaction (montant, canal, date, marchand) ; données de connexion (IP, géolocalisation, OS, version de l'application, durée) ; étiquettes de fraude confirmée |
| **Données sensibles (art. 9)** | Aucune. Aucune variable protégée utilisée (genre, âge, origine) |
| **Destinataires** | Data engineers (toutes zones, accès journalisé) ; data scientists de la Direction des Risques (zone curated uniquement) ; analystes fraude (staging et curated) ; DPO |
| **Transferts hors zone** | Aucun : traitement et stockage locaux (zone UEMOA). Tout hébergement cloud futur devra respecter la réglementation BCEAO / UEMOA et les garanties de l'art. 46 RGPD |
| **Durées de conservation** | RAW : 90 jours · staging : 365 jours · curated et versions DVC : 5 ans (traçabilité des modèles, obligations LCB-FT) · journaux : 1 an |
| **Mesures de sécurité** | Voir ci-dessous |

## Minimisation et protection intégrées (art. 5.1.c et art. 25)

| Donnée | RAW | Staging | Curated (`ml_fraud_features`) |
|---|---|---|---|
| `customer_id` | en clair | pseudonyme HMAC | pseudonyme HMAC (`customer_pseudo`) |
| `device_id` | en clair | pseudonyme HMAC | absent (seules des features agrégées) |
| Adresse IP | complète | réseau /24 | **absente** |
| Latitude / longitude | précises | zone de service (10 villes) | **absentes** (features dérivées seulement) |
| Raison sociale / contact marchand | en clair | **non propagés** | absents |

- **Pseudonymisation** : HMAC-SHA256 avec clé secrète, déterministe (jointures cohérentes entre sources) et non
  réversible sans la clé. Les données restent **personnelles** (art. 4.5) : le RGPD continue de s'appliquer.
- **Clé** : hors du code et de Git (`.env` ignoré ; Variable Airflow masquée). En production : coffre-fort
  (HashiCorp Vault, AWS KMS) détenu par le RSSI, jamais par les utilisateurs des données.
- **Contrôles automatiques bloquants** : schéma exact de la table finale (aucune colonne IP/GPS/identifiant), format
  des pseudonymes, absence d'IP dans le contenu (`quality_out.py`).
- **Accès** : politique de moindre privilège par rôle et par zone (`gouvernance/politique_acces.yaml`).
- **Traçabilité** : `audit_log` (une ligne par étape et par run), journaux horodatés, zone landing, versions DVC.

## Procédure en cas de fuite de la clé de pseudonymisation

1. Révoquer la clé et en générer une nouvelle (coffre-fort) ; suspendre le DAG.
2. Re-pseudonymiser staging et curated (`build_features.py`, idempotent) et produire une nouvelle version DVC.
3. Purger les anciennes versions exposées : `dvc gc` + réécriture des snapshots concernés.
4. Évaluer le risque avec le DPO : notification à l'autorité sous 72 h si risque (art. 33), aux personnes si risque
   élevé (art. 34).

## Droits des personnes

- **Accès / rectification** : via le service client ; la ré-identification d'un pseudonyme passe par une procédure DPO.
- **Effacement (art. 17)** : suppression en RAW, recalcul de staging et curated ; pour les versions DVC antérieures,
  `dvc gc` et réécriture documentée des snapshots. Limite : obligations de conservation LCB-FT (art. 17.3.b).
- **Décision automatisée (art. 22)** : le score de fraude est une **aide à la décision** ; tout blocage durable est revu
  par un analyste (intervention humaine).

## AI Act

La détection de fraude financière est exclue des systèmes à haut risque de l'annexe III (point 5.b). Bonnes pratiques
appliquées malgré tout (art. 10) : qualité des données contrôlée et documentée, absence de variables protégées,
étiquettes sans fuite temporelle, split temporel pour une évaluation réaliste, traçabilité données ↔ code (Git + DVC).
À surveiller : biais géographique possible (`zone_derniere_session_inhabituelle` pénalise les clients mobiles) —
à auditer par segment après déploiement.

## Analyse d'impact (AIPD)

Traitement à grande échelle de données de localisation et de profilage comportemental : une **AIPD (art. 35) est
recommandée** avant la mise en production du modèle.
