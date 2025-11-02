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

def obtener_usuarios(mongoclient):
    db = mongoclient["practica2"]
    return db["usuarios"]
def obtener_peliculas(mongoclient):
    db = mongoclient["practica2"]
    return db["peliculas"]


def agg1(mongoclient):
    peliculas = obtener_peliculas(mongoclient)

    agg1 = peliculas.aggregate([
    {
        '$unwind': {
            'path': '$pais'
        }
    }, {
        '$group': {
            '_id': '$pais', 
            'num_peliculas': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'num_peliculas': -1, 
            '_id': 1
        }
    }
    ])

    return list(agg1) #he usado unwind, preguntar al profe si había otra forma más eficiente


def agg2(mongoclient, pais):
    usuarios = obtener_usuarios(mongoclient)

    agg2 = usuarios.aggregate([
    {
        '$match': {
            'direccion.pais': pais
        }
    }, {
        '$unwind': {
            'path': '$gustos'
        }
    }, {
        '$group': {
            '_id': '$gustos', 
            'total': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'total': -1, 
            '_id': 1
        }
    }, {
        '$limit': 3
    }, {
        '$project': {
            '_id': 0, 
            'total': 1, 
            'tipo': '$_id'
        }
    }
])

    return list(agg2)



def agg3(mongoclient):
    usuarios = obtener_usuarios(mongoclient)

    agg3 = usuarios.aggregate([
    {
        '$match': {
            'edad': {
                '$gt': 17
            }
        }
    }, {
        '$group': {
            '_id': '$direccion.pais', 
            'min_edad': {
                '$min': '$edad'
            }, 
            'max_edad': {
                '$max': '$edad'
            }, 
            'avg_edad': {
                '$avg': '$edad'
            }, 
            'num_usuarios': {
                '$sum': 1
            }
        }
    }, {
        '$match': {
            'num_usuarios': {
                '$gte': 3
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])

    return list(agg3)



def agg4(mongoclient):
    usuarios = obtener_usuarios(mongoclient)

    agg4 = usuarios.aggregate([
    {
        '$unwind': {
            'path': '$visualizaciones'
        }
    }, {
        '$group': {
            '_id': '$visualizaciones.titulo', 
            'num_visualizaciones': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'num_visualizaciones': -1, 
            '_id': 1
        }
    }, {
        '$limit': 10
    }, {
        '$project': {
            '_id': 0, 
            'num_visualizaciones': 1, 
            'titulo': '$_id'
        }
    }
])

    return list(agg4)

#print(agg1(mongoclient))
#print(agg2(mongoclient, "Emiratos Árabes Unidos"))
#print(agg3(mongoclient))
print(agg4(mongoclient))