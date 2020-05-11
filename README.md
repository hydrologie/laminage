# Laminage

laminage est un programme permettant de simuler des centaines de milliers de séries stochastiques d'apports à l'aide du logiciel de gestion des réservoirs HEC ResSim.


## Gestion de projet
- [X] Conversion des fichiers csv vers dss en parallèle
- [ ] Création des alternatives en parallèle (**en cours**)

- Exemple d'une simulation complète : [note de calcul](notebooks/Preparation_et_simulation_HEC_ResSim_stochastique.ipynb) (**en cours**)


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
