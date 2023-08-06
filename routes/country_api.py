from fastapi import APIRouter, HTTPException
import json
from fastapi.responses import JSONResponse
from models.country import Country
from config.db import mycollection
from bson.objectid import ObjectId
from starlette import responses as _responses
from schemas.country_schemas import *


country_api = APIRouter()

######################################## POST ##########################################

# Page d'acceuille
@country_api.get("/", tags=["Countries"])
async def home():
    """
    Returns:
    On redirige vers /redoc pour voir la documentation de l'API.
    redirect: Une redirection vers /redoc.
    """
    return _responses.RedirectResponse(url='/redoc') 


# Documentation de l'endpoint insert_country
@country_api.post("/insert_country/", response_model=dict, tags=["Countries"])
async def insert_country(country: Country):
    """
    Insère un nouveau document de pays dans la collection MongoDB.

    Parameters:
    - country (Country): Un objet Country contenant les informations du pays à insérer.

    Returns:
    dict: Un dictionnaire contenant un message de confirmation et les données insérées.
    """
    country_dict = dict(country)
    mycollection.insert_one(country_dict)
    
    return {"message": "Country inserted successfully", "data": country_dict}



######################################## PUT ##########################################

# Documentation de l'endpoint update_country
@country_api.put("/update_country/{id}", response_model=dict, tags=["Countries"])
async def update_country(id: str, country: Country):
    """
    Met à jour les informations d'un pays dans la collection MongoDB.

    Parameters:
    - id (str): L'ID du document de pays à mettre à jour.
    - country (Country): Un objet Country contenant les nouvelles informations du pays.

    Returns:
    dict: Un dictionnaire contenant un message de confirmation et les données mises à jour.
    """
    country_dict = dict(country)
    updated_country = updateCountry(id, country_dict, mycollection)
    if updated_country is not None:
        return {"message": "Country updated successfully", "data": country_dict}
    else:
        raise HTTPException(status_code=404, detail="Country not found")


######################################## DELETE ##########################################

# Documentation de l'endpoint delete_country
@country_api.delete("/delete_country/{id}", response_model=dict, tags=["Countries"])
async def delete_country(id: str):
    """
    Supprime un document de pays de la collection MongoDB.

    Parameters:
    - id (str): L'ID du document de pays à supprimer.

    Returns:
    dict: Un dictionnaire contenant un message de confirmation et les données supprimées.
    """
    deleted_country = mycollection.delete_one({"_id": ObjectId(id)})
    if deleted_country.deleted_count == 1:
        return {"message": "Country deleted successfully", "data": serializeDict(deleted_country)}
    else:
        raise HTTPException(status_code=404, detail="Country not found")
    


######################################## GET ##########################################

# Documentation de l'endpoint get_countries
@country_api.get("/countries_info/", response_model=dict, tags=["Countries"])
async def get_countries():
    """
    Récupère toutes les informations des pays depuis la collection MongoDB et les renvoie au format JSON.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    df = getAllCountries(mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_country
@country_api.get("/country/{name}", response_model=dict, tags=["Countries"])
async def get_country(name: str):
    """
    Récupère les informations d'un pays depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - name (str): Le nom du pays à récupérer.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données du pays au format JSON.
    """
    df = getCountry(name, mycollection)
    if df is not None: 
        return JSONResponse(content=df.to_dict(orient="records"))
    else:
        raise HTTPException(status_code=404, detail="Country not found")


# Documentation de l'endpoint get_countries_densityD1D2
@country_api.get("/countries_density/{D1}/{D2}", response_model=dict, tags=["Countries"])
async def get_countries_densityD1D2(D1: float, D2: float):
    """
    Récupère les informations de densité (density) de tous les pays dont la densité est comprise 
    entre D1 et D2 depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - D1 (float): La valeur minimale de densité.
    - D2 (float): La valeur maximale de densité.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    df = getCountriesDensityD1D2(D1, D2, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_most_populated_country
@country_api.get("/country_most_populated/{year}", response_model=dict, tags=["Countries"])
async def get_most_populated_country(year: int):
    """
    Récupère le pays le plus peuplé en fonction de l'année depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - year (int): L'année de référence pour laquelle on veut récupérer le pays le plus peuplé.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données du pays le plus peuplé au format JSON.
    """
    df = getMostPopulatedCountry(year, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))

# Documentation de l'endpoint get_least_populated_country
@country_api.get("/country_least_populated/{year}", response_model=dict, tags=["Countries"])
async def get_least_populated_country(year: int):
    """
    Récupère le pays le moins peuplé en fonction de l'année depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - year (int): L'année de référence pour laquelle on veut récupérer le pays le moins peuplé.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données du pays le moins peuplé au format JSON.
    """
    df = getLeastPopulatedCountry(year, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_world_averagePop
@country_api.get("/average_pop/", response_model=dict, tags=["Countries"])
async def get_world_averagePop():
    """
    Récupère la population moyenne mondiale en 1980, 2000, 2010, 2022, 2023, 2030 et 2050 
    depuis la collection MongoDB et les renvoie au format JSON.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données de population moyenne au format JSON.
    """
    df = getWorldAveragePop(mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_countries_pops
@country_api.get("/countries_pop/", response_model=dict, tags=["Countries"])
async def get_countries_pops():
    """
    Récupère les informations de population (pop1980, pop2000, pop2010, pop2022, pop2023, pop2030, pop2050)
      de tous les pays depuis la collection MongoDB et les renvoie au format JSON.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    df = getCountriesPop(mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_countries_areas_sup1_sup2
@country_api.get("/countries_areas_sup1_sup2/{sup1}/{sup2}", response_model=dict, tags=["Countries"])
async def get_countries_areas_sup1_sup2(sup1: float, sup2: float):
    """
    Récupère les informations de surface (area) de tous les pays dont la surface est comprise entre sup1 et sup2 depuis
    la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - sup1 (float): La valeur minimale de surface.
    - sup2 (float): La valeur maximale de surface.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    df = getCountriesAreasSup1Sup2(sup1, sup2, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_aggregated_request pour des requêtes personnalisées
@country_api.get("/custom_aggregation/{query}", response_model=dict, tags=["Countries"])
async def get_aggregated_request(query: str):
    """
    Récupère les informations de tous les pays depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - query (str): La requête personnalisée à effectuer sur la collection MongoDB vous devez saisir sa sous
      forme de liste entre [contenu de la requête].
    exemple : [{"$project": {"country": "$country", "pop1980": "$pop1980","pop2000": "$pop2000",
            "pop2010": "$pop2010", "pop2023": "$pop2023", "pop2030": "$pop2030", "pop2050": "$pop2050" }}]

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    query = json.loads(query)

    df = getAggregationRequest(query, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_find_request pour des requêtes personnalisées
@country_api.get("/custom_find/{query}", response_model=dict, tags=["Countries"])
async def get_find_request(query: str):
    """
    Récupère les informations de tous les pays depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - query (str): La requête personnalisée à effectuer sur la collection MongoDB vous devez saisir sa sous
    forme de liste entre [contenu de la requête].
    exemple : [{"area": {"$gte": 220000, "$lte": 280000}}, {"_id":0}]

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    # convertir string en dictionnaire
    query = json.loads(query)
    df = getFindRequest(query, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))


# Documentation de l'endpoint get_distinct_request pour des requêtes personnalisées
@country_api.get("/custom_distinct/{query}", response_model=dict, tags=["Countries"])
async def get_distinct_request(query: str):
    """
    Récupère les informations de tous les pays depuis la collection MongoDB et les renvoie au format JSON.

    Parameters:
    - query (str): La requête personnalisée à effectuer sur la collection MongoDB.

    Returns:
    JSONResponse: Un objet JSONResponse contenant les données des pays au format JSON.
    """
    #query = json.loads(query)
    df = getDistinctRequest(query, mycollection)
    return JSONResponse(content=df.to_dict(orient="records"))

# Documentation de l'endpoint get_nb_countries_supavg
@country_api.get("/nb_countries_supavg/{year}", response_model=dict, tags=["Countries"])
async def get_nb_countries_supavg(year: int):
    """
    Récupère le nombre de pays dont la population en une année est supérieure à la moyenne mondiale depuis
    a collection MongoDB et les renvoie au format JSON.

    Returns:
    Un  dict contenant le nombre de pays dont la population en une année est supérieure à la moyenne mondiale.
    Sous la forme {"Mondiale average population in " + str(year) : moy,
        "nb_countries_supavg": nb_countries_supavg}
    """
    nb_countries_supavg, moy = getNbCountriesPopSupAvg(year, mycollection)
    
    return {"Mondiale average population in " + str(year) : moy,
        "Number of countries with population grather than average population in " + str(year) : nb_countries_supavg}

# Documentation de l'endpoint get_nb_countries_infavg
@country_api.get("/nb_countries_infavg/{year}", response_model=dict, tags=["Countries"])
async def get_nb_countries_infavg(year: int):
    """
    Récupère le nombre de pays dont la population en une année est inférieure à la moyenne mondiale depuis 
    la collection MongoDB et les renvoie au format JSON.

    Returns:
    Un  dict contenant le nombre de pays dont la population en une année est inférieure à la moyenne mondiale.
    Sous la forme {"Mondiale average population in " + str(year) : moy,
        "nb_countries_infavg": nb_countries_infavg}
    """
    nb_countries_infavg, moy = getNbCountriesPopInfAvg(year, mycollection)
    
    return {"Mondiale average population in " + str(year) : moy,
        "Number of countries with population lower than average population in " + str(year) : nb_countries_infavg}