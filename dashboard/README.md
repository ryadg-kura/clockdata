# ClockData Dashboard

Tableau de bord Streamlit pour visualiser les statistiques de santé de la couche Gold.

## Prérequis

Python 3.10+ et pip.

## Lancement

```bash
cd dashboard
pip install -r requirements.txt
```

Si le pipeline n'a pas encore tourné, générer des données d'exemple :

```bash
python3 generate_sample_data.py
```

Lancer le dashboard :

```bash
streamlit run app.py
```

Ouvrir http://localhost:8501

## Variable d'environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| `GOLD_PATH` | `../data/gold` | Chemin vers le dossier Gold (fichiers `part-*.parquet`) |

```bash
GOLD_PATH=/chemin/vers/gold streamlit run app.py
```

## Tests

```bash
cd dashboard
python3 -m pytest tests/ -v
```

## Données d'exemple

```bash
python3 generate_sample_data.py                  # écrit dans ../data/gold/
python3 generate_sample_data.py /mon/chemin      # chemin personnalisé
```

Génère 3 appareils x 7 jours x 17 heures = 357 lignes.
