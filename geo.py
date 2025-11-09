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
    "collections": {"monumentos": "monumentos", "monumentos_clean": "monumentos_clean"},
}


def obtener_monumentos(mongoclient: MongoClient):
    """Obtiene la colección de monumentos de la base de datos.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de monumentos.
    """
    db = mongoclient[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["monumentos"]]


def obtener_monumentos_clean(mongoclient: MongoClient):
    """Obtiene la colección de monumentos limpios de la base de datos.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        Collection: La colección de monumentos_clean.
    """
    db = mongoclient[MONGO_DATABASE_CONFIG["name"]]
    return db[MONGO_DATABASE_CONFIG["collections"]["monumentos_clean"]]


def agg_clean(mongoclient: MongoClient):
    """Limpia datos de monumentos y crea colección monumentos_clean con índice geoespacial.

    Extrae documentos de @graph, transforma location a formato GeoJSON Point,
    almacena en monumentos_clean y crea índice 2dsphere.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        CommandCursor: Cursor de la agregación.
    """
    monumentos = obtener_monumentos(mongoclient)

    result = monumentos.aggregate(
        [
            {
                "$unwind": {
                    "path": "$@graph",
                }
            },
            {
                "$match": {
                    "@graph.location.longitude": {"$ne": None},
                    "@graph.location.latitude": {"$ne": None},
                }
            },
            {
                "$addFields": {
                    "longitude_double": {"$toDouble": "$@graph.location.longitude"},
                    "latitude_double": {"$toDouble": "$@graph.location.latitude"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "@id": "$@graph.@id",
                    "id": "$@graph.id",
                    "title": "$@graph.title",
                    "relation": "$@graph.relation",
                    "references": "$@graph.references",
                    "address": "$@graph.address",
                    "location": {
                        "type": "Point",
                        "coordinates": ["$longitude_double", "$latitude_double"],
                    },
                    "organization": "$@graph.organization",
                }
            },
            {
                "$out": {
                    "db": MONGO_DATABASE_CONFIG["name"],
                    "coll": MONGO_DATABASE_CONFIG["collections"]["monumentos_clean"],
                }
            },
        ]
    )

    monumentos_clean = obtener_monumentos_clean(mongoclient)
    monumentos_clean.create_index([("location", "2dsphere")])

    return result


def geo_query1(mongoclient: MongoClient, n: float):
    """Encuentra monumentos dentro de n kilómetros de la Facultad de Informática UCM.

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.
        n (float): Distancia máxima en kilómetros.

    Returns:
        Cursor: Cursor con título y calle de monumentos ordenados por cercanía.
    """
    monumentos_clean = obtener_monumentos_clean(mongoclient)

    return monumentos_clean.find(
        {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [-3.73336109904238, 40.45265933919037],
                    },
                    "$maxDistance": n * 1000,
                }
            }
        },
        {"_id": 0, "title": 1, "address.street-address": 1},
    )


def geo_query2(mongoclient: MongoClient):
    """Encuentra monumentos dentro de Ciudad Universitaria (región cuadrada).

    Args:
        mongoclient (MongoClient): El cliente de MongoDB.

    Returns:
        Cursor: Cursor con título y calle de monumentos en Ciudad Universitaria.
    """
    monumentos_clean = obtener_monumentos_clean(mongoclient)

    return monumentos_clean.find(
        {
            "location": {
                "$geoWithin": {
                    "$geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    -3.741817918089083,
                                    40.434407294070,
                                ],  # Esquina inferior izquierda
                                [
                                    -3.725071065360681,
                                    40.434407294070,
                                ],  # Esquina inferior derecha
                                [
                                    -3.725071065360681,
                                    40.45553987197873,
                                ],  # Esquina superior derecha
                                [
                                    -3.741817918089083,
                                    40.45553987197873,
                                ],  # Esquina superior izquierda
                                [
                                    -3.741817918089083,
                                    40.434407294070,
                                ],  # Cerrar el polígono
                            ]
                        ],
                    }
                }
            }
        },
        {"_id": 0, "title": 1, "address.street-address": 1},
    )


if __name__ == "__main__":
    from pprint import pprint
    mongoclient = MongoClient("mongodb://localhost:27017/")

    # Ejemplos de prueba
    print("=== Test agg_clean ===")
    print("Ejecutando limpieza de datos y creación de índice...")
    agg_clean(mongoclient)
    print("Completado.")

    print("\n=== Test geo_query1 ===")
    for monumento in geo_query1(mongoclient, 0.75):
        pprint(monumento)

    print("\n=== Test geo_query2 ===")
    for monumento in geo_query2(mongoclient):
        pprint(monumento)
