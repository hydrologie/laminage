[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hydrologie/laminage/master)

# Laminage

[laminage](https://github.com/hydrologie/laminage) is a simple framework built on top of [Dask](https://dask.org/) to scale the computation of hundreds of thousands of reservoir operations and water planning simulations. Most traditionnal reservoir modelling software (HEC ResSim, RiverWare, MODSIM, etc.) are single-threaded by design and are limited to Windows OS. [laminage](https://github.com/hydrologie/laminage) distributes the calculation of a user-provided HEC-ResSim model over to Dask workers in order to compute up to hundreds of simulations simultaneously either on a multi-core machine or a cluster of nodes. By using WINE, it is also possible to run [laminage](https://github.com/hydrologie/laminage) on Ubuntu and possibly other linux-based OS which is espacially convenient as most clusters run on linux.

# Installation

## Ubuntu 20.04
Git et Anaconda/Miniconda doivent préalablement être installé

```bash
git clone https://github.com/hydrologie/laminage.git
cd laminage

conda env update --name laminage --file environment.yml
```
## Windows



## Utilisation

- le répertoire "laminage" contient le programme général : les classes et les fonctions de chaque composante du laminage. L'utilisateur ne devrait pas avoir à interagir avec ce répertoire.
- le répertoire "notebook" contient l'ensemble des notes de calculs pour réaliser chaque composante de laminage. 
L'utilisateur utilisent ces notes de calculs pour appeler l'engin de calcul de "laminage" afin de réaliser les tâches du projet.
