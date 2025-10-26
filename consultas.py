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
import datetime

def obtener_usuarios(mongoclient):
    db = mongoclient["practica2"]
    return db["usuarios"]
def obtener_peliculas(mongoclient):
    db = mongoclient["practica2"]
    return db["peliculas"]

def usuario_peliculas(mongoclient, user_id, n):
    usuarios = obtener_usuarios(mongoclient)

    pipeline = [
        {"$limit": n}
    ]

    resultadoLimiteN = usuarios.aggregate(pipeline)
    
    pipeline = [
        {"$match": {"_id": user_id}},
        {"$project": {"_id": 0, "email": 1}}
    ]

    resultadoFiltroUser = usuarios.aggregate(pipeline)
    
    data = {
        "visualizaciones": list(resultadoLimiteN),
    }
    lista = [data, list(resultadoFiltroUser)]; # No se imprime exactamente igual que en el PDF

    return lista

def usuarios_gustos(mongoclient, gustos, n):
    usuarios = obtener_usuarios(mongoclient)

    pipeline = [
        {"$match": {"gustos": { "$in": gustos } }},
        {"$project": {"_id": 1, "apellido1": 1, "apellido2": 1, "nombre": 1}},
        {"$limit": n}
    ]

    resultado = usuarios.aggregate(pipeline) # apellido1, apellido2 y nombre no se imprime en el orden del PDF
    return list(resultado)

def usuario_sexo_edad(mongoclient, sexo, edad_min, edad_max ):
    usuarios = obtener_usuarios(mongoclient)

    pipeline = [
        {"$match": {"sexo": sexo, "edad": { "$gt": edad_min - 1, "$lt": edad_max + 1} }},
        {"$project": {"_id": 1}}
    ]

    resultado = usuarios.aggregate(pipeline) # apellido1, apellido2 y nombre no se imprime en el orden del PDF
    return list(resultado)


def usuarios_apellidos(mongoclient):
    usuarios = obtener_usuarios(mongoclient)

    pipeline = [
    {
        '$match': {
            '$expr': {
                '$eq': [
                    '$apellido1', '$apellido2'
                ]
            }
        }
    }, {
        '$sort': {
            'edad': 1
        }
    }, {
        '$project': {
            '_id': 0, 
            'apellido1': 1, 
            'apellido2': 1, 
            'nombre': 1
        }
    }
    ]

    resultado = usuarios.aggregate(pipeline)
    return list(resultado)
    

def pelicula_prefijo(mongoclient, prefijo):
    peliculas = obtener_peliculas(mongoclient)

    pipeline = [
    {
        '$match': {
            'director': re.compile(f"^{re.escape(prefijo)}")
        }
    }, {
        '$project': {
            '_id': 0, 
            'director': 1, 
            'titulo': 1
        }
    }
    ]

    resultado = peliculas.aggregate(pipeline)
    return list(resultado)
    

def usuarios_gustos_numero(mongoclient, n):
    usuarios = obtener_usuarios(mongoclient)

    pipeline = [
    {
        '$match': {
            'gustos': {
                '$size': 3
            }
        }
    }, {
        '$sort': {
            'edad': -1
        }
    }, {
        '$project': {
            'edad': 1, 
            'gustos': 1
        }
    }
]

    resultado = usuarios.aggregate(pipeline)
    return list(resultado)
    

def usuarios_vieron_pelicula(mongoclient, id_pelicula, inicio, fin):
    usuarios = obtener_usuarios(mongoclient)

    fecha_inicio = datetime.datetime.strptime(inicio, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    fecha_fin = datetime.datetime.strptime(fin, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)

    pipeline = [
    {
        '$unwind': '$visualizaciones'
    }, {
        '$project': {
            'visualizaciones.fecha': {
                '$dateFromString': {
                    'dateString': '$visualizaciones.fecha'
                }
            }, 
            'visualizaciones._id': 1
        }
    }, {
        '$match': {
            'visualizaciones.fecha': {
                '$gt': fecha_inicio,
                '$lt': fecha_fin
            }, 
            'visualizaciones._id': ObjectId(id_pelicula)
        }
    }, {
        '$project': {
            '_id': 1
        }
    }
]

    resultado = usuarios.aggregate(pipeline)
    return list(resultado)

#print(usuario_peliculas(mongoclient, "gnoguera", 3))
#print(usuarios_gustos(mongoclient, ["terror", "comedia"], 5))
#print(usuario_sexo_edad(mongoclient, "M", 39, 54 ))
#print(usuarios_apellidos(mongoclient))
#print(pelicula_prefijo(mongoclient, "Yol"))
#print(usuarios_gustos_numero(mongoclient, 3))
print(usuarios_vieron_pelicula(mongoclient, '583ef652323e9572e2814c48',  "1999-01-01", "2002-12-31"))