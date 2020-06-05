# Laminage

laminage est un programme permettant de simuler des centaines de milliers de séries stochastiques d'apports à l'aide du logiciel de gestion des réservoirs HEC ResSim.


## Gestion de projet
- [X] Conversion csv/dss distribué ([JIRA](https://jiraprd03.solutions.hydroquebec.com/browse/DEBIEHH-222))
- [X] Création des alternatives en mode distribué ([JIRA](https://jiraprd03.solutions.hydroquebec.com/browse/DEBIEHH-223))
- [X] Création des simulations en mode distribué
- [X] Appel des simulations en mode distribué
- [X] Compilation des résultats en mode distribué
- [ ] Création des courbes guide en mode distribué (**en cours**)
- [ ] Mise à jour des courbes d'évacuation automatique
- [ ] Mise à jour des courbes d'emmagasinement automatique
- [X] Mise en oeuvre du moteur de calcul du laminage automatique
- [ ] Règles de gestion automatique
- [ ] Tests du laminage stochastique distribué
- [ ] Préparation des fichiers csv d'entrée de Prsim pour Hec-Ressim

Exemple d'une simulation : [note de calcul](notebooks/Preparation_et_simulation_HEC_ResSim_stochastique.ipynb) (**en cours**)



## Installation et configuration de l'environnement

Git et Anaconda/Miniconda doivent préalablement être installé

```bash
git clone https://github.com/hydrologie/laminage.git
cd laminage

conda env update --name laminage --file environment.yml
```

## Utilisation

- le répertoire "laminage" contient le programme général : les classes et les fonctions de chaque composante du laminage. L'utilisateur ne devrait pas avoir à interagir avec ce répertoire.
- le répertoire "notebook" contient l'ensemble des notes de calculs pour réaliser chaque composante de laminage. 
L'utilisateur utilisent ces notes de calculs pour appeler l'engin de calcul de "laminage" afin de réaliser les tâches du projet.
