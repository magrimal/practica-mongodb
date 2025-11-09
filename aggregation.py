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

from pymongo import MongoClient

MONGO_DATABASE_CONFIG = {
    "name": "sgdi_pr2",
    "collections": {"usuarios": "usuarios", "peliculas": "peliculas"},
}


def obtener_usuarios(mongoclient: MongoClient):
    """Obtiene la colección de usuarios de la base de datos.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de usuarios.
    """
    db = mongoclient[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["usuarios"]]


def obtener_peliculas(mongoclient: MongoClient):
    """Obtiene la colección de películas de la base de datos.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de películas.
    """
    db = mongoclient[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["peliculas"]]


def agg1(mongoclient: MongoClient):
    """Listado de país-número de películas, ordenado por número descendente y país ascendente.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        CommandCursor: Cursor con resultados de la agregación.
    """
    peliculas = obtener_peliculas(mongoclient)

    return peliculas.aggregate(
        [
            {"$unwind": {"path": "$pais"}},
            {"$group": {"_id": "$pais", "num_peliculas": {"$sum": 1}}},
            {"$sort": {"num_peliculas": -1, "_id": 1}},
        ]
    )


def agg2(mongoclient: MongoClient, pais: str):
    """Top 3 tipos de película más populares entre usuarios de un país.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.
        pais (str): Nombre del país.

    Returns:
        CommandCursor: Cursor con los 3 tipos más populares.
    """
    usuarios = obtener_usuarios(mongoclient)

    return usuarios.aggregate(
        [
            {"$match": {"direccion.pais": pais}},
            {"$unwind": {"path": "$gustos"}},
            {"$group": {"_id": "$gustos", "total": {"$sum": 1}}},
            {"$sort": {"total": -1, "_id": 1}},
            {"$limit": 3},
            {"$project": {"_id": 0, "total": 1, "tipo": "$_id"}},
        ]
    )


def agg3(mongoclient: MongoClient):
    """Estadísticas de edad por país (solo mayores de edad, mínimo 3 usuarios).

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        CommandCursor: Cursor con min, max, avg de edad y número de usuarios por país.
    """
    usuarios = obtener_usuarios(mongoclient)

    return usuarios.aggregate(
        [
            {"$match": {"edad": {"$gt": 17}}},
            {
                "$group": {
                    "_id": "$direccion.pais",
                    "min_edad": {"$min": "$edad"},
                    "max_edad": {"$max": "$edad"},
                    "avg_edad": {"$avg": "$edad"},
                    "num_usuarios": {"$sum": 1},
                }
            },
            {"$match": {"num_usuarios": {"$gte": 3}}},
            {"$sort": {"_id": 1}},
        ]
    )


def agg4(mongoclient: MongoClient):
    """Top 10 películas más vistas, ordenadas por visualizaciones y título.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        CommandCursor: Cursor con título y número de visualizaciones.
    """
    usuarios = obtener_usuarios(mongoclient)

    return usuarios.aggregate(
        [
            {"$unwind": {"path": "$visualizaciones"}},
            {
                "$group": {
                    "_id": "$visualizaciones.titulo",
                    "num_visualizaciones": {"$sum": 1},
                }
            },
            {"$sort": {"num_visualizaciones": -1, "_id": 1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "num_visualizaciones": 1, "titulo": "$_id"}},
        ]
    )


if __name__ == "__main__":
    from pprint import pprint
    mongoclient = MongoClient("mongodb://localhost:27017/")

    # Ejemplos de prueba
    print("=== Test agg1 ===")
    for result in agg1(mongoclient):
        pprint(result)

    print("\n=== Test agg2 ===")
    for result in agg2(mongoclient, "Emiratos Árabes Unidos"):
        pprint(result)

    print("\n=== Test agg3 ===")
    for result in agg3(mongoclient):
        pprint(result)

    print("\n=== Test agg4 ===")
    for result in agg4(mongoclient):
        pprint(result)
