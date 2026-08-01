# Dernière requête

Date : 01/08/2026

Demande : corriger les skills de l’onglet « Assistant IA DHIS2 RDC » afin qu’un canevas PPTX joint soit actualisé avec les données DHIS2 sans modifier sa structure, sans toucher aux diapositives hors DHIS2 (notamment PTF, volontairement vide), sans ajouter de graphiques ou diapositives non demandés, et avec un vrai tableau comparatif du score qualité. Prétester l’Assistant IA/Kimi et inspecter sa production avant de conclure.

Fichiers concernés : `docs/index.html`, `scripts/test-ia-pptx-canevas.mjs`, `scripts/test-ia-kimi-pptx-policy.mjs`, `scripts/test_ia_ollama.mjs`, `project_memory/*`.

Statut : TERMINÉ localement — mode structure stricte et transactionnel, nouvelle opération `remplacer_forme_par_tableau`, barème DPS déterministe via `ctx.scoreQualiteDps`, contrôles finaux bloquants, protection des diapositives hors DHIS2, limite Kimi de 12 étapes/45 minutes. Prétest moteur : 49 → 49 diapositives, seule D35 modifiée, D6 PTF identique, tableau score qualité visible. Prétest réel Kimi K3 : PASS (`requete_dhis2` ×2 puis `modifier_presentation`), aucun ajout interdit. Non déployé dans cette session.
