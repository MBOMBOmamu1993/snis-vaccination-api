# Dernière requête

Date : 02/08/2026

Demande : retirer toute limite de 30 étapes de l’Assistant IA ; limiter les offres client aux codes de 20, 30, 40, 50 et 100 USD avec montant libre à partir de 10 USD ; intégrer dans les skills IA tous les UID vérifiés par l’audit `Audit_indicateurs_DHIS2_Assistant_IA_Kasa-Vubu_2026.xlsx` et contrôler les indicateurs vides du canevas `Revue_Semestrielle_S1_2026_ZS_Kasa_Vubu.pptx`, notamment ECV, réunions CODESA et réunions de validation ; pousser et déployer en production.

Règles métier confirmées : Proportion ECV (%) = somme du dataElement `M2JQW0H44dI` sur toute la période sélectionnée ÷ nourrissons survivants de la même période × 100. Réunions CODESA = indicator `N3HHnz0Waos`. Réunions de validation/hebdomadaires ECZ = indicator `zLIRMEWlQXy`, avec lecture des opérandes DHIS2 si le canevas demande les nombres prévus/tenus.

Implémentation : catalogue local généré depuis l’audit (481 lignes + ECV confirmé, 393 UID audit + ECV, 76 programIndicators EVENT dans 4 programmes, 83 lignes à configurer), tool `rechercher_uid_canevas` et helper `ctx.uidSearch`, programme EVENT via leurs UID exacts, règle d’affichage des valeurs partielles au lieu de N/D, ECV ajouté au calcul PEV direct, offres et validation serveur mises à jour dans les trois modes de paiement. La boucle agentique reste sans limite globale de durée ou d’étapes.

Statut : DÉPLOYÉ EN PRODUCTION. Tests catalogue/ECV/EVENT/RVV/achats, dry-run Worker et prétest navigateur PPTX réussis (49→49 diapositives, seule D35 modifiée, PTF identique). Fusion GitHub `4bcfcf39`, GitHub Pages et Vercel vérifiés avec le catalogue HTTP 200 ; Worker `pev-ia-proxy` version `969c7008-f60c-412d-9f75-0593a7c4bac3`.
