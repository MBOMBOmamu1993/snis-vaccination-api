# Dernière requête

Date : 01/08/2026

Demande : corriger les skills de l’onglet « Assistant IA DHIS2 RDC » afin qu’un canevas PPTX joint soit actualisé avec les données DHIS2 sans modifier sa structure, sans toucher aux diapositives hors DHIS2 (notamment PTF, volontairement vide), sans ajouter de graphiques ou diapositives non demandés, et avec un vrai tableau comparatif du score qualité. Prétester l’Assistant IA/Kimi et inspecter sa production avant de conclure.

Fichiers concernés : `docs/index.html`, `scripts/test-ia-pptx-canevas.mjs`, `scripts/test-ia-kimi-pptx-policy.mjs`, `scripts/test_ia_ollama.mjs`, `project_memory/*`.

Statut : CORRECTIF URGENT VALIDÉ — le délai global de 45 minutes et la limite Kimi spéciale de 12 étapes ont été entièrement supprimés après interruption réelle d’un canevas au moment de son écriture. Kimi dispose des 30 étapes communes, sans arrêt chronométré, et doit écrire le PPTX par lots `telecharger:false` dès qu’une section est calculée. Le mode structure stricte, le tableau score qualité et la protection PTF restent actifs. Prétest moteur : PASS, 49 → 49 diapositives, seule D35 modifiée, D6 PTF identique.
