# Autor: Joshua Juste NIKIEMA
import pandas as pd
from bson.objectid import ObjectId


def countryEntity(item) -> dict:
    return {
        "id": str(item["_id"]),
        "country": item["country"],
        "rank": item["rank"],
        "area": item["area"],
        "landAreaKm": item["landAreaKm"],
        "cca2": item["cca2"],
        "cca3": item["cca3"],
        "netChange": item["netChange"],
        "growthRate": item["growthRate"],
        "worldPercentage": item["worldPercentage"],
        "density": item["density"],
        "densityMi": item["densityMi"],
        "place": item["place"],
        "pop1980": item["pop1980"],
        "pop2000": item["pop2000"],
        "pop2010": item["pop2010"],
        "pop2022": item["pop2022"],
        "pop2023": item["pop2023"],
        "pop2030": item["pop2030"],
        "pop2050": item["pop2050"],
    }

def countriesEntity(entity) -> list:
    return [countryEntity(item) for item in entity]

def serializeDict(x) -> dict:
    return {**{i:str(x[i]) for i in x if i=='_id'}, **{i:x[i] for i in x if i!='_id'}}

def serializeList(entity) -> list:
    return [serializeDict(x) for x in entity]


# Modifier les données d'un pays
def updateCountry(id, newCountryData, mycollection):
    
    for key, value in newCountryData.items():
        if value != None:
            upt_co = mycollection.find_one_and_update({"_id": ObjectId(id)}, {"$set": {key: value}})
            
    return upt_co
    
    
# Fonction pour recupérer les pays dont la densité est compris entre d1 et d2 et les mettre dans un dataframe
def getCountriesDensityD1D2(d1, d2, mycollection):
    d1 = float(d1)
    d2 = float(d2)
    # On récupère les pays
    countries = mycollection.find({"density": {"$gte": d1, "$lte": d2}})
    # On récupère les données de densité de population
    data = []
    for country in countries:
        country_data = {
            "Country": country["country"],
            "Density": country.get("density")
        }
        data.append(country_data)
    # On met les données dans un dataframe
    df = pd.DataFrame(data)
    return df

# Obtenir la population moyenne  du monde en 1980, 2000, 2010, 2022, 2023, 2030 et 2050 en utilisant une requête d'agrégation
def getWorldAveragePop(mycollection):
    # On récupère la densité de population moyenne du monde
    worldAverageDensity = mycollection.aggregate([
        {"$group": {
            "_id": "World Average Density",
            "PopulationMoyEn1980": {"$avg": "$pop1980"},
            "PopulationMoyEn2000": {"$avg": "$pop2000"},
            "PopulationMoyEn2010": {"$avg": "$pop2010"},
            "PopulationMoyEn2022": {"$avg": "$pop2022"},
            "PopulationMoyEn2023": {"$avg": "$pop2023"},
            "PopulationMoyEn2000": {"$avg": "$pop2030"},
            "PopulationMoyEn2050": {"$avg": "$pop2050"}
        }}
    ])
    # On récupère les données de population
    data = []
    for country in worldAverageDensity:
        country_data = {
            "PopulationMoyEn1980": country.get("PopulationMoyEn1980"),
            "PopulationMoyEn2000": country.get("PopulationMoyEn2000"),
            "PopulationMoyEn2010": country.get("PopulationMoyEn2010"),
            "PopulationMoyEn2022": country.get("PopulationMoyEn2022"),
            "PopulationMoyEn2023": country.get("PopulationMoyEn2023"),
            "PopulationMoyEn2030": country.get("PopulationMoyEn2030"),
            "PopulationMoyEn2050": country.get("PopulationMoyEn2050")
        }
        data.append(country_data)
    # On met les données dans un dataframe
    df = pd.DataFrame(data)
    return df

# Récuperer toutes les données de la collection
def getAllCountries(mycollection):
    # On récupère les pays
    countries = mycollection.find({})

    colonnes_a_convertir = ["area", "landAreaKm", "netChange", "growthRate", "worldPercentage", "density"]
    # On met les données dans un dataframe
    df = pd.DataFrame(serializeList(countries))
    df[colonnes_a_convertir] = df[colonnes_a_convertir].astype(str)
    return df

# Récuperer une ligne spécifique de la collection en fonction du nom du pays
def getCountry(countryName, mycollection):
    # On récupère le pays
    country = mycollection.find_one({"country": countryName}, {"_id": 0})
    if country:
        # On met les données dans un dataframe
        df = pd.DataFrame(serializeDict(country), index=[0])
        return df
    else:
        return None

# Trouver le pays le moins peuplé en fonction de l'année
def getLeastPopulatedCountry(year, mycollection):
    year = str(year)
    # On récupère le pays le moins peuplé en fonction de l'année
    country = mycollection.find_one(sort=[("pop"+year, 1)])
    # On récupère les données de population
    data = []
    if country:
        country_data = {
            "Country": country["country"],
            "Population in "+year: country.get("pop"+year)
        }
        data.append(country_data)
    # On met les données dans un dataframe
    df = pd.DataFrame(data)
    return df

# Trouver le pays le plus peuplé en fonction de l'année
def getMostPopulatedCountry(year, mycollection):
    year = str(year)
    # On récupère le pays le plus peuplé en fonction de l'année
    country = mycollection.find_one(sort=[("pop"+year, -1)])
    # On récupère les données de population
    data = []
    if country:
        country_data = {
            "Country": country["country"],
            "Population in "+year: country.get("pop"+year)
        }
        data.append(country_data)
    # On met les données dans un dataframe
    df = pd.DataFrame(data)
    return df

# Récuperer le paays dont la superficie est comprise entre sup1 et sup2
def getCountriesAreasSup1Sup2(sup1, sup2, mycollection):
    sup1 = float(sup1)
    sup2 = float(sup2)
    # On récupère les pays
    countries = mycollection.find({"area": {"$gte": sup1, "$lte": sup2}})
    # On récupère les données de superficie
    data = []
    for country in countries:
        country_data = {
            "Country": country["country"],
            "Area": country.get("area")
        }
        data.append(country_data)
    # On met les données dans un dataframe
    df = pd.DataFrame(data)
    return df

# Fonction de requêtes pour extraire la population de 1980, 2000, 2010, 2023 et 2050 des pays grace une requête d'agrégation
def getCountriesPop(mycollection):
    # On récupère les données de population
    countries = mycollection.aggregate([
        {"$project": {
            "country": "$country",
            "pop1980": "$pop1980",
            "pop2000": "$pop2000",
            "pop2010": "$pop2010",
            "pop2023": "$pop2023",
            "pop2030": "$pop2030",
            "pop2050": "$pop2050"
        }}
    ])
    # On met les données dans un dataframe
    df = pd.DataFrame(serializeList(countries))
    return df

# Fonction pour recevoir n'importe quelle requête d'agrégation et retourner le résultat dans un dataframe
def getAggregationRequest(aggregation, mycollection):
    # On récupère les données de population
    countries = mycollection.aggregate(aggregation)
    # On met les données dans un dataframe
    df = pd.DataFrame(serializeList(countries))
    return df

# Fonction pour recevoir n'importe quelle requête "find" et retourner le résultat dans un dataframe
def getFindRequest(query, mycollection):
    if len(query) == 1:
        # On récupère les données de population
        countries = mycollection.find(query[0])
    elif len(query) == 2:
        countries = mycollection.find(query[0], query[1])   
    # On met les données dans un dataframe
    df = pd.DataFrame(serializeList(countries))
    return df

# Fonction pour recevoir n'importe quelle requête 'distinct' et retourner le résultat dans un dataframe
def getDistinctRequest(distinct, mycollection):
    # On récupère les données de population
    countries = mycollection.distinct(distinct)
    # On met les données dans un dataframe
    df = pd.DataFrame(countries)
    return df

# Fonction pour trouver le nombre de pays dont la population en une année donnée est supérieure à la population moyenne
def getNbCountriesPopSupAvg(year, mycollection):
    year = str(year)

    # On récupère la population moyenne
    avg = mycollection.aggregate([
        {"$group": {
            "_id": "World Average Population",
            "avg": {"$avg": "$pop"+year}
        }}
    ])

    # Ou convertir le curseur en une liste de documents Python
    avg = list(avg)
    avg = avg[0]["avg"]
    
    # On récupère le nombre de pays dont la population est supérieure à la population moyenne
    nbCountries = mycollection.count_documents({"pop"+year: {"$gte": avg}})
    return nbCountries, avg

# Fonction pour trouver le nombre de pays dont la population en une année donnée est inférieure à la population moyenne
def getNbCountriesPopInfAvg(year, mycollection):
    year = str(year)

    # On récupère la population moyenne
    avg = mycollection.aggregate([
        {"$group": {
            "_id": "World Average Population",
            "avg": {"$avg": "$pop"+year}
        }}
    ])

    # Ou convertir le curseur en une liste de documents Python
    avg = list(avg)
    avg = avg[0]["avg"]
    
    # On récupère le nombre de pays dont la population est inférieure à la population moyenne
    nbCountries = mycollection.count_documents({"pop"+year: {"$lte": avg}})
    return nbCountries, avg