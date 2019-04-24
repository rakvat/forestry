# Forest Industry Data To Play with

## Setup

```
conda env create -f=requirements.txt -n forestry
# install packages with pip if not available in conda
. activate forestry
```

## Data

The CSV data in `/data` is from fao.org.

It is imported into MySQL with

```
python run_import.py
```


## Linting

```
pylint *.py
```

## Tests

```
python -m unittest test/*.py
```

## ETL with Bonobo experiments

```
pip install bonbo
python etl.py
bonobo inspect --graph etl.py | dot -Tpng -o etl.png
```


