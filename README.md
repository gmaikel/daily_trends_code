# daily_trends_code

Ce repository contient deux implémentations d’un pipeline *daily trends* (apple-music-demo) :
- Une version **PySpark** : daily_trends_pyspark
- Une version **Snowpark Python** : daily_trends_snowpark

Les deux projets sont basés sur le pipeline de démonstration original en scala : *daily-trends-apple-music-demo*.

---

## Structure du projet

Chaque implémentation contient :
- Un fichier `requirements.txt` listant les dépendances nécessaires :
  ```bash
  pip install -r requirements.txt
  ```

- Les dossiers suivants doivent être créés manuellement pour le bon fonctionnement du pipeline :
  ```
  data/
  ├── source/              # Données sources provenant du DSP
  ├── destination/         # Données de sortie après traitement
  └── data_repository/     # Données de référence et de mapping
  ```

---

## Exécution locale

Pour tester le pipeline en local, il faut exécuter le fichier `all_jobs.py` situé dans :

```
PROJET/src/all_jobs.py
```

### Exemple de commande :

```bash
python src/all_jobs.py \
  --source data/source \
  --destination data/destination \
  --DSP apple-music-demo \
  --date 2022-05-10 \
  --distributor believe \
  --id_sup_platform 94 \
  --report_id 1 \
  --report_iteration 1 \
  --local \
  --local-catalog data/data_repository
```

### Explication des arguments :
- `--source` : chemin vers les données brutes
- `--destination` : chemin de sortie des données transformées
- `--DSP` : nom du DSP à traiter
- `--date` : date du rapport
- `--distributor` : nom du distributeur
- `--id_sup_platform` : identifiant de la plateforme
- `--report_id` : identifiant du rapport
- `--report_iteration` : version du rapport
- `--local` : exécution en mode local
- `--local-catalog` : chemin vers le dossier data

---

## Remarques
- Assurez-vous que vos credentials Snowflake sont bien configurés pour l’exécution en Snowpark Python.

---

## Checklist (TODO)

- [x] Implémentation PySpark
- [x] Implémentation partielle Snowpark Python
- [ ] Finir l'implémentation complète du dossier common
- [ ] Finalisation des tests unitaires
- [ ] Orchestration
- [ ] Packaging
- [ ] Documentation technique complète


