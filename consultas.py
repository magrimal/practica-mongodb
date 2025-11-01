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

    usuariosPeliculas = usuarios.find({"_id": user_id}, {"visualizaciones": {"$slice":n}, "_id":0,"visualizaciones":1, "email":1})

    return list(usuariosPeliculas)


def usuarios_gustos(mongoclient, gustos, n):
    usuarios = obtener_usuarios(mongoclient)

    usuariosGustos = usuarios.find({"gustos": {"$all": gustos}}, {"_id":1,"apellido1":1,"apellido2":1,"nombre":1}).limit(n)

    return list(usuariosGustos) #¿en el pdf imprime 3 y son 5?


def usuario_sexo_edad(mongoclient, sexo, edad_min, edad_max ):
    usuarios = obtener_usuarios(mongoclient)

    usuariosSexoEdad = usuarios.find({"sexo": sexo, "edad": { "$gt": edad_min - 1, "$lt": edad_max + 1}}, {"_id:1"})

    return list(usuariosSexoEdad)

def usuarios_apellidos(mongoclient):
    usuarios = obtener_usuarios(mongoclient)

    usuariosApellidos = usuarios.find({"$expr": { "$eq": ["$apellido1", "$apellido2"]}}, {"_id":0,"apellido1":1,"apellido2":1,"nombre":1}).sort({"edad":1})
    
    return list(usuariosApellidos) #apellido1 y apellido2 no se imprimen en el mismo orden que el pdf
    

def pelicula_prefijo(mongoclient, prefijo):
    peliculas = obtener_peliculas(mongoclient)

    peliculaPrefijo = peliculas.find({"director": re.compile(f"^{re.escape(prefijo)}")}, {"_id":0,"director":1,"titulo":1})

    return list(peliculaPrefijo)
    

def usuarios_gustos_numero(mongoclient, n):
    usuarios = obtener_usuarios(mongoclient)

    usuariosGustosNumero = usuarios.find({"gustos":{"$size":n}}, {"edad":1,"gustos":1}).sort({"edad":-1})

    return list(usuariosGustosNumero) #edad se imprime como tercer elemento, a diferencia del pdf que se imprime en el segundo
    

def usuarios_vieron_pelicula(mongoclient, id_pelicula, inicio, fin):
    usuarios = obtener_usuarios(mongoclient)

    usuariosVieronPeliculas = usuarios.find({"visualizaciones": { "$elemMatch": {"fecha": { "$gte": inicio, "$lt": fin } ,"_id": ObjectId(id_pelicula)} }}, {"_id": 1 })

    return list(usuariosVieronPeliculas) #orden de impresión no es igual al pdf

#print(usuario_peliculas(mongoclient, "gnoguera", 3))
print(usuarios_gustos(mongoclient, ["terror", "comedia"], 5))
#print(usuario_sexo_edad(mongoclient, "M", 50, 80 ))
#print(usuarios_apellidos(mongoclient))
#print(pelicula_prefijo(mongoclient, "Yol"))
#print(usuarios_gustos_numero(mongoclient, 6))
#print(usuarios_vieron_pelicula(mongoclient, '583ef652323e9572e2814c48',  "1999-01-01", "2002-12-31"))