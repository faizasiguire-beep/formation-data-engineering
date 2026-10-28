#!/usr/bin/env bash
# =====================================================================================
# Crée une version DVC du jeu de données final (ml_fraud_features) et la nomme par un tag Git.
#
#   bash jour3/projet_integre/scripts/versionner.sh data-v1 "Premier jeu de features"
#
# À lancer depuis la racine du projet, environnement « data » activé, APRÈS un run réussi du pipeline
# (la table doit avoir passé le contrôle de sortie). Le code doit être commité : sinon la version de
# données ne serait pas reliée à une version de code reproductible.
# =====================================================================================
set -euo pipefail
TAG="${1:?usage : versionner.sh <tag> \"<description>\"}"
DESC="${2:?usage : versionner.sh <tag> \"<description>\"}"
P=jour3/projet_integre
SNAP=$P/data/ml_fraud_features

if ! git diff --quiet HEAD -- . ':!'"$SNAP"; then
  echo "⚠ Du code ou des paramètres ne sont pas commités : commitez avant de versionner les données."; exit 1
fi
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "⚠ Le tag $TAG existe déjà."; exit 1
fi

python "$P/src/export_snapshot.py" > /dev/null          # instantané déterministe de la table validée
dvc add "$SNAP"                                         # hash + copie dans le cache DVC
LIGNES=$(python -c "import json; m=json.load(open('$SNAP/MANIFEST.json')); print(m['lignes'], 'lignes,', m['fraudes'], 'fraudes')")
git add "$SNAP.dvc" "$P/data/.gitignore"
git commit -qm "data: $TAG — $DESC ($LIGNES)${COMMIT_TRAILER:+

$COMMIT_TRAILER}"
git tag -a "$TAG" -m "$DESC ($LIGNES)"
dvc push                                                # envoi vers le stockage distant
echo "✔ $TAG : $LIGNES — commit $(git rev-parse --short HEAD), hash DVC $(grep -m1 'md5' "$SNAP.dvc" | awk '{print $3}')"
