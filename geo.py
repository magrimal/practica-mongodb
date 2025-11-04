"""
Asignatura: SGDI
Práctica 2:
Autores: 
- MARIA JOSÉ GRIMALDI FERNÁNDEZ
- NATALIA PALOMA LA ROSA MONTERO
- IREMAR LUHETSY RIVAS ÁLVAREZ

Declaración de integridad
Declaramos que esta solución es fruto exclusivamente de nuestro trabajo 
personal. No hemos sido ayudados por ninguna otra persona o sistema automático
ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido 
nuestra solución con otras personas de manera directa o indirexcta. 
Declaramos además que no hemos realizado de manera deshonesta ninguna otra 
actividad que pueda mejorar nuestros resultados ni perjudicar los resultados 
de los demás.
"""

from bson import ObjectId
from pymongo import MongoClient
mongoclient = MongoClient("mongodb://localhost:27017/")
import pandas as pd
import re

def obtener_monumentosciudadmadrid(mongoclient): #cambiar el nombre de la bd sgdi_pr2
    db = mongoclient["practica2"]
    return db["monumentosciudadmadrid"]

def obtener_monumentosciudadmadrid_toclean(mongoclient): #cambiar el nombre de la bd sgdi_pr2
    db = mongoclient["practica2"]
    return db["monumentos_clean"]

def agg_clean(mongoclient):
    monumentos = obtener_monumentosciudadmadrid(mongoclient) 

    agg_clean = monumentos.aggregate([
    {
        '$unwind': {
            'path': '$@graph'
        }
    }, {
        '$match': {
            '@graph.location.longitude': {
                '$ne': None
            }, 
            '@graph.location.latitude': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            '@id': '$@graph.@id', 
            'id': '$@graph.id', 
            'title': '$@graph.title', 
            'relation': '$@graph.relation', 
            'references': '$@graph.references', 
            'address': '$@graph.address', 
            'area': '$@graph.area', 
            'locality': '$@graph.locality', 
            'postal-code': '$@graph.postal-code', 
            'street-address': '$@graph.stree-address', 
            'location': {
                'type': 'Point', 
                'coordinates': [
                    '$@graph.location.longitude', '$@graph.location.latitude'
                ]
            }, 
            'organization': '$@graph.organization'
        }
    }, {
        '$out': {
            'db': 'practica2', 
            'coll': 'monumentos_clean'
        }
    }
])
    
    monumentos_clean = obtener_monumentosciudadmadrid_toclean(mongoclient) 

    monumentos_clean.create_index( { "location": "2dsphere" })

    return agg_clean #he usado unwind, preguntar al profe si había otra forma más eficiente

#'coordinates': [
#                {'$convert': {'input': '$@graph.location.longitude', 'to': 'double'}},
#                {'$convert': {'input': '$@graph.location.latitude', 'to': 'double'}}
#            ]

def geo_query1(mongoclient, n):
    monumentos_clean = obtener_monumentosciudadmadrid_toclean(mongoclient)

    monumentos_encontrados = monumentos_clean.find(
        {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [-3.73336109904238, 40.45265933919037]
                    },
                    "$maxDistance": n * 1000  
                }
            }
        },
        {"_id": 0, "title": 1, "address.street-address": 1}
    )

    resultados = list(monumentos_encontrados)
    
    return resultados

def geo_query2(mongoclient):
    monumentos_clean = obtener_monumentosciudadmadrid_toclean(mongoclient)

    monumentos_encontrados = monumentos_clean.find(
        {
            "location": {
                "$within": {
                    "$polygon": [[-3.741817918089083,40.434407294070],[-3.741817918089083,40.45553987197873],
                                  [-3.725071065360681,40.45553987197873],[-3.725071065360681,40.434407294070]]
                }
            }
        },
        {"_id": 0, "title": 1, "address.street-address": 1}
    )

    resultados = list(monumentos_encontrados)
    
    return resultados

#print(list(agg_clean(mongoclient)))
#print(list(geo_query1(mongoclient,0.75)))
print(list(geo_query2(mongoclient))) #No se imprime en el mismo orden que el pdf