#!/bin/bash
# Script pour effectuer un push sécurisé avec rebase et tentatives multiples

BRANCH=${1:-main}
MAX_RETRIES=3

for i in $(seq 1 $MAX_RETRIES); do
  echo "Tentative de synchronisation et push (essai $i/$MAX_RETRIES)..."
  git pull --rebase origin "$BRANCH" || true
  if git push origin "HEAD:$BRANCH"; then
    echo "Push réussi !"
    exit 0
  fi
  echo "Le push a échoué. Attente de 5 secondes avant nouvel essai..."
  sleep 5
done

echo "Erreur : Impossible de pousser les modifications après $MAX_RETRIES tentatives."
exit 1