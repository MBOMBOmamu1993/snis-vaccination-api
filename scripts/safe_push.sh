#!/bin/bash
# Push sécurisé avec rebase et tentatives multiples.
#
# Les fichiers de données sont des .gz binaires : git ne sait PAS les fusionner.
# Quand deux workflows écrivent en parallèle (ex. un backfill qui chevauche le
# sync quotidien), un `git pull --rebase` nu s'arrête sur un conflit binaire.
# L'ancienne version masquait l'échec avec `|| true` puis poussait un état de
# rebase incomplet → les données fraîchement générées étaient PERDUES
# silencieusement (bug : le backfill Td 2+ corrigé n'atterrissait pas).
#
# Correctif : le rebase utilise `-X theirs`, qui — en mode rebase — conserve
# NOS commits (les données que ce run vient de générer) sur tout fichier en
# conflit. C'est le bon choix : ce run porte le fetch le plus récent. Tout
# échec de rebase est détecté (plus de `|| true` masquant) : on abandonne le
# rebase et on réessaie proprement, et le script échoue si rien ne passe.

set -o pipefail
BRANCH=${1:-main}
MAX_RETRIES=5

for i in $(seq 1 "$MAX_RETRIES"); do
  echo "Tentative de synchronisation et push (essai $i/$MAX_RETRIES)..."

  # Stasher toute modif non indexée (ex. bit exécutable posé par chmod +x)
  STASHED=0
  if ! git diff --quiet || ! git diff --cached --quiet; then
    if git stash push -u -m "safe_push_auto_$$" >/dev/null 2>&1; then
      STASHED=1
      echo "Modifications locales mises de côté (stash)."
    fi
  fi

  git fetch origin "$BRANCH"

  # Rebase en préférant NOS données sur les conflits (binaires .gz inclus).
  if ! git rebase -X theirs "origin/$BRANCH"; then
    echo "Rebase en conflit malgré -X theirs — abandon et nouvel essai."
    git rebase --abort 2>/dev/null || true
    [ "$STASHED" = "1" ] && git stash pop >/dev/null 2>&1 || true
    sleep 5
    continue
  fi

  [ "$STASHED" = "1" ] && git stash pop >/dev/null 2>&1 || true

  if git push origin "HEAD:$BRANCH"; then
    echo "Push réussi !"
    exit 0
  fi
  echo "Le push a échoué (l'amont a bougé). Attente 5 s avant nouvel essai..."
  sleep 5
done

echo "Erreur : impossible de pousser après $MAX_RETRIES tentatives."
exit 1
