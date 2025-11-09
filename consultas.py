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
nuestra solución con otras personas de manera directa o indirecta.
Declaramos además que no hemos realizado de manera deshonesta ninguna otra
actividad que pueda mejorar nuestros resultados ni perjudicar los resultados
de los demás.
"""

from bson import ObjectId
from pymongo import MongoClient
import re

MONGO_DATABASE_CONFIG = {
    "name": "sgdi_pr2",
    "collections": {"usuarios": "usuarios", "peliculas": "peliculas"},
}


def obtener_usuarios(mongo_client: MongoClient):
    """Obtiene la colección de usuarios de la base de datos.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de usuarios.
    """
    db = mongo_client[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["usuarios"]]


def obtener_peliculas(mongo_client: MongoClient):
    """Obtiene la colección de películas de la base de datos.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de películas.
    """
    db = mongo_client[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["peliculas"]]


def usuario_peliculas(mongo_client: MongoClient, user_id: str, n: int):
    """Obtiene el email y las n primeras visualizaciones de películas de un usuario.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        user_id (str): El ID del usuario.
        n (int): El número de películas a obtener.

    Returns:
        Cursor: Un cursor con el email y visualizaciones del usuario.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {"_id": user_id}, {"visualizaciones": {"$slice": n}, "email": 1, "_id": 0}
    )


def usuarios_gustos(mongo_client: MongoClient, gustos: list, n: int):
    """Devuelve los primeros n usuarios que tienen todos los gustos especificados.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        gustos (list): Lista de gustos que deben tener los usuarios.
        n (int): Número máximo de usuarios a devolver.

    Returns:
        Cursor: Un cursor con _id, nombre y apellidos de los usuarios.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {"gustos": {"$all": gustos}},
        {"_id": 1, "nombre": 1, "apellido1": 1, "apellido2": 1},
    ).limit(n)


def usuario_sexo_edad(
    mongo_client: MongoClient, sexo: str, edad_min: int, edad_max: int
):
    """Obtiene los usuarios de un sexo y rango de edad específicos.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        sexo (str): El sexo de los usuarios ('M' o 'F').
        edad_min (int): Edad mínima (incluida).
        edad_max (int): Edad máxima (incluida).

    Returns:
        Cursor: Un cursor con los _id de los usuarios que cumplen los criterios.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {"sexo": sexo, "edad": {"$gte": edad_min, "$lte": edad_max}}, {"_id": 1}
    )


def usuarios_apellidos(mongo_client: MongoClient):
    """Devuelve usuarios cuyos apellidos coinciden, ordenados por edad ascendente.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.

    Returns:
        Cursor: Un cursor con nombre y apellidos de los usuarios.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {"$expr": {"$eq": ["$apellido1", "$apellido2"]}},
        {"_id": 0, "nombre": 1, "apellido1": 1, "apellido2": 1},
    ).sort("edad", 1)


def pelicula_prefijo(mongo_client: MongoClient, prefijo: str):
    """Recupera películas cuyo director tiene un nombre que empieza por un prefijo.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        prefijo (str): Prefijo del nombre del director.

    Returns:
        Cursor: Un cursor con título y director de las películas.
    """
    peliculas = obtener_peliculas(mongo_client)

    return peliculas.find(
        {"director": re.compile(f"^{re.escape(prefijo)}")},
        {"_id": 0, "titulo": 1, "director": 1},
    )


def usuarios_gustos_numero(mongo_client: MongoClient, n: int):
    """Obtiene usuarios que tienen exactamente n gustos, ordenados por edad descendente.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        n (int): Número exacto de gustos.

    Returns:
        Cursor: Un cursor con _id, edad y gustos de los usuarios.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {"gustos": {"$size": n}}, {"_id": 1, "edad": 1, "gustos": 1}
    ).sort("edad", -1)


def usuarios_vieron_pelicula(
    mongo_client: MongoClient, id_pelicula: str, inicio: str, fin: str
):
    """Devuelve usuarios que vieron una película en un rango de fechas.

    Args:
        mongo_client (MongoClient): El cliente de MongoDB.
        id_pelicula (str): El ID de la película (como string).
        inicio (str): Fecha de inicio en formato 'YYYY-MM-DD'.
        fin (str): Fecha de fin en formato 'YYYY-MM-DD' (no incluida).

    Returns:
        Cursor: Un cursor con los _id de los usuarios.
    """
    usuarios = obtener_usuarios(mongo_client)

    return usuarios.find(
        {
            "visualizaciones": {
                "$elemMatch": {
                    "_id": ObjectId(id_pelicula),
                    "fecha": {"$gte": inicio, "$lt": fin},
                }
            }
        },
        {"_id": 1},
    )


if __name__ == "__main__":
    from pprint import pprint

    mongo_client = MongoClient("mongodb://localhost:27017/")

    # Ejemplos de prueba
    print("\n=== Test usuario_peliculas ===")
    for usuario in usuario_peliculas(mongo_client, "fernandonoguera", 3):
        pprint(usuario)

    print("=== Test usuarios_gustos ===")
    for usuario in usuarios_gustos(mongo_client, ["terror", "comedia"], 5):
        pprint(usuario)

    print("\n=== Test usuario_sexo_edad ===")
    for usuario in usuario_sexo_edad(mongo_client, "M", 50, 80):
        pprint(usuario)

    print("\n=== Test usuarios_apellidos ===")
    for usuario in usuarios_apellidos(mongo_client):
        pprint(usuario)

    print("\n=== Test pelicula_prefijo ===")
    for pelicula in pelicula_prefijo(mongo_client, "Yol"):
        pprint(pelicula)

    print("\n=== Test usuarios_gustos_numero ===")
    for usuario in usuarios_gustos_numero(mongo_client, 6):
        pprint(usuario)

    print("\n=== Test usuarios_vieron_pelicula ===")
    for usuario in usuarios_vieron_pelicula(
        mongo_client, "583ef652323e9572e2814c48", "1999-01-01", "2002-12-31"
    ):
        pprint(usuario)
