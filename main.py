import pandas as pd
import pandasql as ps
from pandasql import sqldf
from fastapi import FastAPI


app = FastAPI()


#Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN. 
@app.get("/get_max_duration/{year}/{platform}/{duraction_type}")
def get_max_duration(year:int, platform, duration_type:str):
    #Cargamos los datos
    df = pd.read_csv('Data/Plataformas.csv')

    #iteramos el filtro de Platform
    if platform == 'amazon':
        plat = 'a%'
    elif platform == 'disney':
        plat = 'd%'
    elif platform == 'hulu':
        plat = 'h%'
    elif platform == 'netflix':
        plat = 'n%'
    else:
        return ("Plataforma incorrecta")
    
    #Hacemos la consulta
    query = f"SELECT title, duration_int, duration_type FROM df WHERE type = 'movie' AND date_added LIKE '%{year}%' AND ID LIKE '{plat}' AND duration_type = '{duration_type}' "
    
    result = sqldf(query, locals())

    # Retorna la película con mayor duración
    return result.loc[result['duration_int'].idxmax()]


#Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año (la función debe llamarse get_score_count(platform, scored, year))
@app.get("/get_score_count/{platform}/{scored}/{year}")
def get_score_count(platform:str, scored:float, year:str):

    df = pd.read_csv('Data/Plataformas.csv')

    # Iteramos el filtro de plataforma
    if platform.lower() == 'amazon':
        platform_id = 'a%'
    elif platform.lower() == 'disney':
        platform_id = 'd%'
    elif platform.lower() == 'hulu':
        platform_id = 'h%'
    elif platform.lower() == 'netflix':
        platform_id = 'n%'
    else:
        return "Plataforma incorrecta"

    query = f"SELECT COUNT(*) AS count FROM df WHERE ID LIKE '{platform_id}%' AND type='movie' AND rating >= {scored} AND date_added BETWEEN '{year}-01-01' AND '{year}-12-31'"


    return ps.sqldf(query, locals())


#Cantidad de películas por plataforma con filtro de PLATAFORMA. (La función debe llamarse get_count_platform(platform))
@app.get("/get_count_platform/{platform}")
def get_count_platform(platform: str):
    df = pd.read_csv('Data/Plataformas.csv')

    if platform == 'amazon':
        plat = 'a'
    elif platform == 'disney':
        plat = 'd'
    elif platform == 'hulu':
        plat = 'h'
    elif platform == 'netflix':
        plat = 'n'
    else:
        return ("Plataforma incorrecta")

    query = f"SELECT COUNT(*) AS count FROM df WHERE ID LIKE '{plat}%' AND type = 'movie'"
    return ps.sqldf(query, locals())


@app.get("/get_actor/{platform}/{year}")
def get_actor(platform: str, year: str):
    # Cargar el archivo CSV en un DataFrame
    df = pd.read_csv('Data/Plataformas.csv')

    # Filtrar por plataforma
    if platform.lower() == 'netflix':
        df = df[df['ID'].str.startswith('n')]
    elif platform.lower() == 'amazon':
        df = df[df['ID'].str.startswith('a')]
    elif platform.lower() == 'hulu':
        df = df[df['ID'].str.startswith('h')]
    elif platform.lower() == 'disney':
        df = df[df['ID'].str.startswith('d')]
    else:
        return 'Plataforma incorrecta'

    # Convertir la columna 'release_year' de float a int
    df['release_year'] = df['release_year'].fillna(0).astype(int)

    # Eliminar las filas con valores nulos en la columna 'cast'
    df = df.dropna(subset=['cast'])

    # Filtrar por año y obtener el actor que más se repite
    cast_count = df.loc[df['release_year'] == int(year)].groupby('cast').size().reset_index(name='count')
    actor = cast_count.loc[cast_count['count'].idxmax(), 'cast']

    return actor