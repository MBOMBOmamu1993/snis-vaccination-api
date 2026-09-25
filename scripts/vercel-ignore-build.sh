#!/bin/bash
# Ignored Build Step de Vercel (snis-vaccination-dhis2) : exit 0 = sauter, exit 1 = construire.
# Les commits du bot ne touchent que les données (servies par GitHub Pages via DATA_ORIGIN) :
# on ne reconstruit que si un fichier réellement copié dans dist/ par build-static.mjs a changé.
# Toute situation douteuse => on construit (jamais d'erreur qui bloque la production).

PATHS="docs/index.html docs/pev-logo.svg docs/pev-logo-officiel.png docs/canevas_revue_formative_pev.pptx docs/data/ia docs/mashako-assets docs/.nojekyll scripts/build-static.mjs scripts/vercel-ignore-build.sh vercel.json"
BASE="${VERCEL_GIT_PREVIOUS_SHA:-}"

if [ -z "$BASE" ]; then
  echo "Aucun déploiement de référence : construction."
  exit 1
fi

# Le clone de Vercel est superficiel (~10 commits) : après une série de commits du bot,
# le commit du dernier déploiement réussi n'y est plus. On le récupère sans les fichiers.
if ! git cat-file -e "$BASE^{commit}" 2>/dev/null; then
  git fetch --quiet --depth=1 --filter=blob:none origin "$BASE" 2>/dev/null \
    || git fetch --quiet --depth=1 --filter=blob:none https://github.com/MBOMBOmamu1993/snis-vaccination-api.git "$BASE" 2>/dev/null \
    || { echo "Commit de référence $BASE introuvable : construction."; exit 1; }
fi

git diff --quiet "$BASE" HEAD -- $PATHS
case $? in
  0) echo "Aucun fichier du site modifié depuis $BASE : construction sautée."; exit 0 ;;
  1) echo "Fichiers du site modifiés depuis $BASE : construction."; exit 1 ;;
  *) echo "Comparaison impossible avec $BASE : construction."; exit 1 ;;
esac
