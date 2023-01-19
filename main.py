from fastapi import FastAPI
import pandas as pd
from pandasql import sqldf

# Cargamos la base de datos limpia
db =pd.read_csv('./Datasets/dataET.csv')

app=FastAPI()


@app.get("/")
def read_root():
    return 'Proyecto Individual P1 para Henry - elaborado por: Julian Agudelo'


# Cargamos información sobre la API
@app.get('/about')
async def about():
    return 'API creada con FastAPI para consulta de datos de ciertas plataformas de Streaming'


@app.get("/get_word_count/{plataforma},{keyword}")
def get_word_count(plataforma:str,keyword:str):
    """ Está función genera una búsqueda que arroja la cantidad de veces que aparece 
        una keyword en el título de películas/series, por plataforma
    Args: 
        plataforma (str): Nombre de la plataforma a buscar  
        keyword (str): Nombre de la palabara clave contenida en el titulo de la película o serie

    Returns: cantidad de veces que aparece una keyword en el título de películas/series, por plataforma
    """
    dic_plataforma ={"netflix":"n","amazon":"a","disney":"d","hulu":"h"}
    id_value = dic_plataforma[plataforma]+"%"
    keyword="%"+keyword+"%"
    consulta1 = f"SELECT COUNT(title) FROM db WHERE title LIKE '{keyword}' AND id LIKE '{id_value}' "
    return (plataforma,sqldf(consulta1).to_string(index=False,header=False))


@app.get("/get_score_count/{plataforma},{score_input},{year}")
def get_score_count(plataforma:str,score_input:int,year:int):
    """ Está función genera una búsqueda de la cantidad de películas por plataforma 
        con un puntaje mayor a XX en determinado año
    Args: 
        plataforma (str): Nombre de la plataforma a buscar  
        score_imput (int): Puntaje mínimo asignado a una pelicula
        year (int): año 
    Returns: cantidad de peliculas que cumplen la condicion
    """
    dic_plataforma ={"netflix":"n","amazon":"a","disney":"d","hulu":"h"}
    id_value = dic_plataforma[plataforma]+"%"
    consulta2 = f"SELECT COUNT(*) FROM db WHERE id LIKE '{id_value}' AND score > '{score_input}' AND release_year = '{year}' AND type LIKE '%movie%'"
    return (plataforma,sqldf(consulta2).to_string(index=False,header=False))


@app.get("/get_second_score/{plataforma}")
def get_second_score(plataforma:str):
    """ Filtrar la segunda película con mayor score para una plataforma determinada, 
    según el orden alfabético de los títulos.
    Args: 
        plataforma (str): Nombre de la plataforma a buscar  
    Returns: segunda película que cumple con las condiciones de filtrado
    """
    dic_plataforma ={"netflix":"n","amazon":"a","disney":"d","hulu":"h"}
    id_value = dic_plataforma[plataforma]+"%"
    consulta3 = f"SELECT title,score FROM db WHERE id LIKE '{id_value}' AND type LIKE '%movie%' ORDER BY score DESC,title ASC, title LIMIT 1 OFFSET 1"
    return (sqldf(consulta3).to_string(index=False,header=False))


@app.get("/get_longest/{plataforma},{type},{year}")
def get_longest(plataforma:str,type:str,year:int):
    """ Película que más duró según año, plataforma y tipo de duración
    Args: 
        plataforma (str): Nombre de la plataforma a buscar 
        type(str): tipo de duración{min,season} 
        year (int): año 

    Returns: Pelicula que mas duro segun año
    """
    dic_plataforma ={"netflix":"n","amazon":"a","disney":"d","hulu":"h"}
    id_value = dic_plataforma[plataforma]+"%"
    type = "%"+type+"%"
    consulta4 = f"SELECT title, max(duration_int),duration_type FROM db WHERE id LIKE '{id_value}' AND release_year = '{year}' AND duration_type LIKE '{type}'"
    return (sqldf(consulta4).to_string(index=False,header=False))


@app.get("/get_rating_count/{rating_input}")
def get_rating_count(rating_input):
    """ Cantidad de series y películas por rating
    Args: 
        rating_input (str): rating pelicula

    Returns: Cantidad de series y peliculas por rating
    """
    consulta5 = f"SELECT COUNT (*) FROM db WHERE rating lIKE '{rating_input}'"
    return (rating_input, sqldf(consulta5).to_string(index=False,header=False))