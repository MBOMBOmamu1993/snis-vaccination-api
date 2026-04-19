#!/bin/bash
# Script pour effectuer un push sécurisé avec rebase et tentatives multiples.
# Met de côté (stash) les modifications non indexées (ex: bit exécutable posé
# par "chmod +x" dans le workflow) avant le rebase, pour éviter l'erreur
# "cannot pull with rebase: You have unstaged changes".

BRANCH=${1:-main}
MAX_RETRIES=3

for i in $(seq 1 $MAX_RETRIES); do
  echo "Tentative de synchronisation et push (essai $i/$MAX_RETRIES)..."

  # Stasher toute modif non indexée pour permettre le rebase
  STASHED=0
  if ! git diff --quiet || ! git diff --cached --quiet; then
    if git stash push -u -m "safe_push_auto_$$" >/dev/null 2>&1; then
      STASHED=1
      echo "Modifications locales mises de côté (stash)."
    fi
  fi

  git fetch origin "$BRANCH" || true
  git pull --rebase origin "$BRANCH" || true

  # Restaurer le stash (si on en a créé un)
  if [ "$STASHED" = "1" ]; then
    git stash pop >/dev/null 2>&1 || true
  fi

  if git push origin "HEAD:$BRANCH"; then
    echo "Push réussi !"
    exit 0
  fi
  echo "Le push a échoué. Attente de 5 secondes avant nouvel essai..."
  sleep 5
done

echo "Erreur : Impossible de pousser les modifications après $MAX_RETRIES tentatives."
exit 1
