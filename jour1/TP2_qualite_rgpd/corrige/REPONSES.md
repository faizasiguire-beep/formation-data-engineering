# TP2 — Éléments de réponse

**Constats attendus de l'audit (A1/A2)**
| Constat | Dimension | Ordre de grandeur |
|---|---|---|
| 100 doublons de `customer_id` (50 exacts, 50 avec email en MAJUSCULES) | unicité | 2 % |
| Emails manquants | complétude | 3 % |
| Emails en majuscules ou accentués | validité | ~1,5 % |
| Dates de naissance en JJ/MM/AAAA ou JJ-MM-AAAA, dates impossibles (2031-02-30) | validité | ~20 % |
| Villes en MAJUSCULES | cohérence | 4 % |
| Consentement : `oui`, `Oui`, `NON`, vide… | cohérence | ~50 % non conformes |
| Revenus à -1, âges > 100 ans | exactitude | ~1 % |
| Transactions : montants manquants / négatifs, devise `xof`, statut `SUCCES` | validité / exactitude | < 0,5 % |

**Faux positifs / faux négatifs du scan RGPD (A3)**
- Faux positifs : `raw_merchants.name` et `merchant_name` sont des raisons sociales (personnes morales),
  hors RGPD… **sauf** pour un entrepreneur individuel dont le nom commercial est son nom propre.
  `contact_email` d'une entreprise peut être nominatif (`prenom.nom@societe.ci`) → donnée personnelle.
- Faux négatifs possibles : `customer_id` est un **identifiant indirect** (il permet de relier toutes les
  données d'une personne) mais n'est pas détecté ; `device_id` est détecté par son nom, mais un champ
  libre (commentaire d'un conseiller) contenant un nom ne le serait pas.
- Conclusion : la détection automatique **assiste** l'analyse, elle ne remplace pas la revue humaine
  documentée dans le catalogue et validée par le DPO.

**Débrief**
1. **RAW non modifiée** : elle est la preuve de ce qui a été reçu et permet de rejouer toute la chaîne si
   une règle de nettoyage était fausse. Corriger en place détruit l'information et l'auditabilité.
2. **Seuil bloquant en sortie** : la RAW reflète la source, ses défauts sont *attendus* ; ce qui doit être
   garanti, c'est ce que consomment les utilisateurs (staging/curated). Bloquer sur la RAW arrêterait le
   pipeline tous les jours sans rien protéger. Sur la RAW, on surveille plutôt les **dérives** (un taux de
   nulls qui passe de 3 % à 40 % = incident source à signaler).
3. **k-anonymat** : sur `[birth_year, gender, city, income_band]`, k vaut 1 → certaines combinaisons sont
   uniques, donc ré-identifiantes par recoupement. La staging est **pseudonymisée, pas anonyme**. Pour un
   jeu partageable : remplacer `birth_year` par `age_band`, regrouper les villes en pays… on obtient seulement
   k = 2 sur `[age_band, gender, country]` : il faut encore supprimer ou regrouper (ex. retirer `gender`,
   fusionner les tranches extrêmes) pour viser k ≥ 5. Démonstration concrète du coût de l'anonymisation.
4. **`gender`** : aucune justification métier pour la fraude, risque de discrimination (variable protégée).
   RGPD art. 22 (décision automatisée produisant des effets juridiques) et AI Act art. 10 (qualité, biais
   des données) → l'exclure des features et vérifier l'absence de biais par groupe a posteriori.
5. **Clé de pseudonymisation** : détenue par la sécurité (RSSI) dans un coffre (HashiCorp Vault, AWS KMS,
   Azure Key Vault), jamais par les utilisateurs des données. Fuite = la pseudonymisation tombe (attaque par
   dictionnaire redevient possible) → rotation de clé et re-pseudonymisation, notification éventuelle (art. 33).
   Perte = plus aucune jointure possible avec les nouvelles données → la clé se sauvegarde.

**Erreurs fréquentes**
- Hash SHA-256 « nu » présenté comme anonymisation.
- Pseudonymiser sans normaliser (`A@x.com` ≠ `a@x.com` → deux pseudonymes pour un même client).
- `drop_duplicates()` sans clé → les quasi-doublons survivent.
- Lignes invalides supprimées sans trace (pas de quarantaine).
- `to_sql` sans schéma : tout en `TEXT`, aucune clé primaire, aucun index.
