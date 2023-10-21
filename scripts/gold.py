import pandas as pd

## TODO: CONTINUAR ESSA FUNCAO
def gold_transform():

    films = pd.read_csv("./data/silver/films.csv")
    peoples = pd.read_csv("./data/silver/peoples.csv")
    planets = pd.read_csv("./data/silver/planets.csv")

    print(films.columns)
    print(peoples.columns)
    print(planets.columns)


if __name__ == "__main__":
    gold_transform()