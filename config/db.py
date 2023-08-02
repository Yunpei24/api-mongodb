from pymongo import MongoClient


url = "mongodb+srv://nikiemajoshua24:MongoDBJosh1234@joshuacluster.ju8f7xt.mongodb.net/?retryWrites=true&w=majority"


client = MongoClient(url)

db = client['world-database']
mycollection = db['global-country-data']