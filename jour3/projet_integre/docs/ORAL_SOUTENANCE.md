# Oral de soutenance — 10 minutes, 4 intervenants

Plan imposé : besoin et sources (1 min) · architecture et démonstration (3 min) · panne provoquée et reprise dans
Airflow (2 min) · conformité (2 min) · versions DVC et reproduction de la v1 (1 min) · limites et suites (1 min).

| Temps | Partie | Qui |
|---|---|---|
| 0:00 → 1:00 | Besoin et sources | [Prénom 1] |
| 1:00 → 4:00 | Architecture et démonstration | [Prénom 2] |
| 4:00 → 6:00 | Panne provoquée et reprise | [Prénom 3] |
| 6:00 → 8:00 | Conformité | [Prénom 4] |
| 8:00 → 9:00 | Versions DVC et reproduction de la v1 | [Prénom 4] |
| 9:00 → 10:00 | Limites et suites | [Prénom 1] |

Les passages entre crochets *[…]* sont des gestes, pas du texte à dire. Rythme visé : environ 130 mots par minute.

---

## 1. Besoin et sources — [Prénom 1] — 1 min

« Bonjour, nous sommes [Prénom 1], [Prénom 2], [Prénom 3] et [Prénom 4].

La Direction des Risques de NexaPay nous a demandé d’améliorer le modèle de fraude construit au TP4. Ce modèle ne
regardait que les transactions : le montant, l’heure, le canal. Or un fraudeur se trahit surtout par son comportement
dans l’application : il se connecte depuis une autre ville, avec un téléphone que le client n’a jamais utilisé,
quelques minutes avant de payer.

Notre livrable est donc une table, `ml_fraud_features`, avec une ligne par transaction et des signaux comportementaux.

Pour la construire, nous combinons quatre sources de trois types. La base core banking fournit les transactions et
les fraudes confirmées, qui arrivent plusieurs jours après la transaction. Une API REST donne le niveau de risque des
marchands, avec OAuth2, pagination et quotas. Et la source nouvelle, non utilisée en TP : le fichier Parquet des
46 000 sessions de l’application, avec l’appareil, l’adresse IP et la position GPS. C’est elle qui porte le
comportement, et aussi les données les plus sensibles.

[Prénom 2] va vous présenter l’architecture. »

---

## 2. Architecture et démonstration — [Prénom 2] — 3 min

### Architecture (1 min)

« Le pipeline suit trois zones.

La zone **RAW** stocke les quatre sources telles quelles. La collecte est incrémentale : on garde un filigrane, une
sorte de marque-page, qui n’avance qu’après un chargement réussi. Un premier contrôle Great Expectations, 26 règles,
établit un diagnostic de cette zone, sans bloquer : ses défauts viennent de la source et sont attendus.

La zone **STAGING** nettoie les données, envoie les lignes irrécupérables en quarantaine avec leur motif, et
pseudonymise les identifiants.

La zone **CURATED** contient la table finale : neuf signaux de session, calculés uniquement à partir des connexions
antérieures à la transaction, pour qu’il n’y ait aucune fuite du futur. Là, un second contrôle de 35 règles est
bloquant : si une seule échoue, rien n’est livré. La table est ensuite versionnée avec DVC.

Le tout est orchestré par un DAG Airflow de 13 tâches, qui tourne chaque nuit à 3 heures. Je vous le montre. »

### Démonstration (2 min)

*[Onglet Airflow ouvert. Cliquer sur le DAG `projet_fraude_comportementale`, puis sur **Trigger**.]*

« Le DAG commence par vérifier que l’API répond, puis collecte les quatre sources en parallèle. »

*[Vue **Graph** : commenter pendant que les tâches passent au vert.]*

« Les collectes écrivent l’une après l’autre dans SQLite, grâce à un pool à une seule place. Viennent ensuite le
contrôle d’entrée, la construction des features avec la pseudonymisation, puis le contrôle de sortie. »

*[Ouvrir le log de `qualite_sortie`.]*

« Le contrôle de sortie est conforme : 31 règles Great Expectations et 4 contrôles transverses. Les transactions ont
moins de 24 heures, les pseudonymes sont cohérents entre sources à 100 %, et il n’y a aucune adresse IP dans la
table. »

*[Ouvrir l’onglet **XCom** de `bilan_run`.]*

« Le bilan du run : 62 350 transactions, dont 479 fraudes. Et si l’on relance ce DAG, rien ne change dans les tables :
la collecte ne trouve rien de nouveau et la staging est remplacée en une seule transaction. Nous l’avons vérifié : on
obtient exactement la même empreinte de contenu.

[Prénom 3] va maintenant casser le pipeline. »

> Si la démo plante : donner les chiffres de mémoire (62 350 transactions, 479 fraudes, contrôle conforme) et passer
> la main sans s’attarder.

---

## 3. Une panne provoquée et sa reprise dans Airflow — [Prénom 3] — 2 min

### La panne (1 min)

*[Vue **Grid** : montrer le run en échec préparé avant la soutenance.]*

« Nous avons provoqué une panne réaliste : l’export quotidien des sessions n’est pas arrivé.

Vous voyez que la collecte des sessions est en rouge, mais que les trois autres collectes ont réussi : chaque source
est isolée. Toutes les étapes en aval sont en orange, `upstream_failed` : elles n’ont pas tourné. Rien de faux n’a été
livré. »

*[Ouvrir le log de `collecte_sessions`.]*

« Airflow a réessayé trois fois, d’abord après 30 secondes, puis après 60 : c’est le backoff exponentiel, utile pour une
panne passagère. Ici, le fichier manquait toujours, donc la tâche a échoué définitivement. »

*[Dans le terminal :]* `tail -1 logs/alertes_projet_fraude.jsonl`

« Le callback d’échec a écrit une alerte : le DAG, la tâche, le numéro de tentative et l’erreur, sans jamais afficher
de secret. »

### La reprise (1 min)

*[Dans le terminal :]* `ls -l data/sources/app_sessions.parquet`

« La cause est corrigée : l’export est arrivé. »

*[Dans **Grid** : cliquer sur la case rouge de `collecte_sessions` → **Clear** → cocher **Downstream** → confirmer.]*

« Nous ne relançons pas tout le DAG : seulement la tâche en échec et celles qui en dépendent. Les trois collectes déjà
réussies ne sont pas refaites.

Pendant que ça tourne, une précision : nous distinguons deux cas. Une panne passagère, comme un réseau coupé ou un
fichier en retard, se règle par les retries automatiques. Une donnée non conforme, elle, ne se corrige pas en
réessayant. C’est pourquoi la construction des features et le contrôle de sortie ont zéro retry : l’alerte part tout
de suite et un humain intervient. Et comme chaque étape est idempotente, la reprise ne crée aucun doublon. »

*[Quand le run est vert :]* « Le pipeline a repris exactement là où il s’était arrêté. [Prénom 4] va vous parler de
conformité. »

---

## 4. Conformité : ce qui sort, ce qui ne sort pas — [Prénom 4] — 2 min

« Les sessions contiennent des données personnelles : l’adresse IP et la position GPS. Le brief interdit de les laisser
sortir dans la table finale. Voici le chemin de chaque donnée.

En zone RAW, tout est en clair : c’est la preuve de ce que nous avons reçu. Cette zone est réservée aux data engineers
et conservée 90 jours.

Dès la staging, l’identifiant client et l’identifiant appareil sont remplacés par un pseudonyme HMAC, calculé avec une
clé secrète. L’adresse IP est tronquée : on garde le réseau, pas la machine. La position GPS devient la ville la plus
proche. Le nom des marchands n’est pas propagé.

Ce qui **sort** dans la table finale : un pseudonyme client et neuf signaux agrégés, par exemple « la dernière
connexion venait-elle d’une autre ville ? ». Ce qui **ne sort pas** : ni IP, ni coordonnées, ni identifiant appareil,
ni aucun identifiant en clair.

Ces règles ne reposent pas sur la bonne volonté : elles sont vérifiées à chaque run par le contrôle de sortie
bloquant. Le schéma de la table est vérifié colonne par colonne, et on cherche en plus toute adresse IP dans le
contenu : résultat, zéro. Le pseudonyme d’un client est le même dans les transactions et dans les sessions, puisqu’on
utilise la même clé : 100 % des clients se retrouvent. On peut donc relier les deux sources sans connaître le vrai
identifiant.

Précision importante : c’est une pseudonymisation, pas une anonymisation. Les données restent personnelles, et le
RGPD s’applique toujours. Nous l’avons donc documenté : un catalogue décrit les 106 colonnes avec leur catégorie RGPD,
et une fiche de registre de l’article 30 précise la finalité, la base légale, qui est l’intérêt légitime de lutte
contre la fraude, les durées de conservation et la procédure en cas de fuite de la clé.

Enfin, nous n’utilisons aucune variable protégée, comme le genre ou l’âge, et aucun secret n’est dans le code ni dans
Git. »

---

## 5. Versions DVC et reproduction de la v1 — [Prénom 4] — 1 min

« Une base de données change en permanence : on ne peut pas la versionner telle quelle. Nous exportons donc une photo
de la table finale, toujours identique pour un même contenu, et c’est elle que DVC versionne. Git garde le code et un
petit pointeur ; DVC garde le contenu, identifié par son empreinte.

Nous avons deux versions : la v1, avec 60 358 transactions, et la v2, après l’arrivée de 2 000 nouvelles transactions.
Je reviens à la v1. »

*[Dans le terminal :]*
```bash
git checkout data-v1 && dvc checkout
cat jour3/projet_integre/data/ml_fraud_features/MANIFEST.json
```

« 60 358 lignes, empreinte `eac5d425…` : nous avons retrouvé exactement les données et le code de la v1. Même si l’on
perdait le poste, `dvc pull` les restaurerait à l’identique. Nous l’avons testé. »

*[Revenir à la v2 :]* `git checkout main && dvc checkout`

« [Prénom 1] va conclure. »

---

## 6. Limites et suites — [Prénom 1] — 1 min

« Pour conclure : est-ce que ça sert ? Oui. Avec le même modèle qu’au TP4 et un découpage temporel, ajouter nos
signaux de session fait passer la PR-AUC de 0,054 à 0,593 : onze fois mieux. Le signal le plus fort : la dernière
connexion venait d’une autre ville pour 60 % des fraudes, contre 0,3 % des transactions légitimes.

Nous voyons aussi les limites. Les données sont simulées : les écarts réels seront plus faibles. SQLite n’accepte
qu’un seul écrivain à la fois. Les features sont recalculées entièrement chaque nuit, ce qui ne tiendrait pas avec mille
fois plus de données. Et le signal « autre ville » peut pénaliser les clients qui voyagent : il faudra l’auditer.

Les suites : passer sur PostgreSQL, ce qui ne demande que de changer une variable ; calculer les features de façon
incrémentale, en SQL ou avec Spark ; passer au scoring en temps réel avec Kafka, comme au TP5 ; et mener une analyse
d’impact RGPD avant la mise en production.

Merci, nous sommes prêts pour vos questions. »

---

## Questions (5 min) — qui répond

| Sujet | Qui |
|---|---|
| Besoin, valeur métier, limites | [Prénom 1] |
| Architecture, features, idempotence (« je relance le DAG deux fois ») | [Prénom 2] |
| Pannes, retries, anomalie inattendue, volume × 1 000 | [Prénom 3] |
| RGPD, fuite de la clé, versions DVC | [Prénom 4] |

Réponses détaillées : `docs/SOUTENANCE.md`.

---

## Préparation, 15 minutes avant (une seule personne)

Depuis `~/formation-data-engineering` :

1. Terminal 1 : `.venv/bin/uvicorn mock_api.app:app --port 8000`
2. Terminal 2 :
   ```bash
   source .venv-airflow/bin/activate && export DE_PYTHON="$(pwd)/.venv/bin/python" && export DAGS_FOLDER="$(pwd)/jour3/projet_integre/dags" && source jour2/TP3_airflow/airflow_env.sh && airflow standalone
   ```
3. Terminal 3 : `.venv/bin/python -m fil_rouge.generate_data --append 300` (activité récente, sinon le contrôle de
   fraîcheur bloque).
4. Préparer la panne : `mv data/sources/app_sessions.parquet data/sources/app_sessions.parquet.absent`, déclencher le
   DAG dans l’interface, attendre que `collecte_sessions` soit en échec (environ 2 minutes).
5. **Remettre le fichier** : `mv data/sources/app_sessions.parquet.absent data/sources/app_sessions.parquet`
   (sinon la démonstration de [Prénom 2] échouera aussi).
6. Laisser ouverts : l’onglet Airflow (http://localhost:8080) et le terminal 3, à la racine du projet.
