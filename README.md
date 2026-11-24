# projectlabs

Polymarket datasets, one folder per category. Each folder has a notebook that builds its CSV:

| Folder | Notebook | Dataset |
| --- | --- | --- |
| `crypto/` | `crypto.ipynb` | `crypto.csv` |
| `politics/` | `politics.ipynb` | `politics.csv` |
| `sports/` | `sports.ipynb` | `sports.csv` |
| `tech/` | `tech.ipynb` | `tech.csv` |

Every CSV has the columns `slug, category, volume, close_date, price_7d, price_3d, price_1d, outcome`.

Run a notebook from inside its own folder: it saves its raw downloads and its CSV next to itself.
