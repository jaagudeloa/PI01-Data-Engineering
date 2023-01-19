#Se importan las diferentes bases de datos desde unos archivos tipo csv y se importan a dataframes con pandas
import pandas as pd
df_amazon= pd.read_csv("Datasets/amazon_prime_titles-score.csv")
df_disney= pd.read_csv("Datasets/disney_plus_titles-score.csv")
df_hulu = pd.read_csv("Datasets/hulu_titles-score (2).csv")
df_netflix= pd.read_csv("Datasets/netflix_titles-score.csv")

#Función para crear id 
def create_id(df,plataforma):

    """ Está funcíon genera una columna id. 
        Cada id se compondrá de la primera letra del nombre de la plataforma, 
        seguido del show_id ya presente en los datasets 
        (ejemplo para títulos de Amazon = as123)  
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 
        plataforma (str): Nombre de la plataforma correspondiente 

    Returns: Dataframe transformado. 
    """

    df["plataforma"]=plataforma
    df["id"] = df["plataforma"].str[0] + df["show_id"]
    first_column = df.pop('id')
    df.insert(0,"id",first_column)
    df.drop("plataforma",axis=1,inplace=True)
    return df

#Ejecutar las funcion create id 
df_amazon =create_id(df_amazon,"amazon")
df_disney =create_id(df_disney,"disney")
df_hulu = create_id(df_hulu,"hulu")
df_netflix= create_id(df_netflix,"netflix")

#función para reemplazar nulos por "G"
def fillna(df):

    """ Está funcíon reemplaza los nulos del campo rating por el
        string "G"   
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 

    Returns: Dataframe transformado. 
    """

    df["rating"].fillna("G",inplace=True)
    return df

#Ejecutar las funciones fillna 
df_amazon =fillna(df_amazon)
df_disney =fillna(df_disney)
df_hulu = fillna(df_hulu)
df_netflix= fillna(df_netflix)

#función lower case
def lower_case(df):
    """ Está funcíon convierte los campos de texto en minúsculas   
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 
    Returns: Dataframe transformado. 
    """

    list_of_columns = ["id","show_id","type","title",
            "director","cast","country",
            "date_added","rating",
            "duration","listed_in","description"]

    for i in list_of_columns:
        df[i] = df[i].str.lower()
    return df

#Se convierte el campo cast de la tabla hulu a tipo string
df_hulu["cast"] = df_hulu["cast"].astype(str)

#Ejecutar las funciones lower_case
df_amazon =lower_case(df_amazon)
df_disney =lower_case(df_disney)
df_hulu = lower_case(df_hulu)
df_netflix= lower_case(df_netflix)

#Definir función Trim
def func_trim(df):
    """ Está funcíon elimina los espacios en blanco en las cadenas de texto   
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 

    Returns: Dataframe transformado. 
    """

    df_obj = df.select_dtypes(["object"])
    df[df_obj.columns]=df_obj.apply(lambda x: x.str.strip())
    return df

#Ejecutar función trim
df_amazon =func_trim(df_amazon)
df_disney =func_trim(df_disney)
df_hulu = func_trim(df_hulu)
df_netflix= func_trim(df_netflix)

#Definir funcion conertir duración de tiempo
def convert_duration(dataframe):
    """ Está funcíon convierte el campo duration en dos campos: 
        duration_int y duration_type. El primero será un integer 
        y el segundo un string indicando la unidad de medición 
        de duración: min (minutos) o season (temporadas)
    
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 

    Returns: Dataframe transformado. 
    """
    dataframe["duration_int"] = dataframe["duration"].str.extract("(\d+)").astype('Int64')
    dataframe["duration_type"] = dataframe["duration"].str.extract("(\D+)")
    return dataframe

#Ejecutar función convertir duración de tiempo
df_amazon =convert_duration(df_amazon)
df_disney =convert_duration(df_disney)
df_hulu = convert_duration(df_hulu)
df_netflix= convert_duration(df_netflix)

#función replace time
def replace_time(df):
    """ Está funcíon convierte el campo de fechas, 
        al formato AAAA-mm-dd
    
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 

    Returns: Dataframe transformado. 
    """
    df["date_added"] = pd.to_datetime(df_amazon["date_added"], format='%B %d, %Y')
    return df

#Ejecutar función replace time
df_amazon =replace_time(df_amazon)
df_disney =replace_time(df_disney)
df_hulu = replace_time(df_hulu)
df_netflix= replace_time(df_netflix)

#funcion replace seasons --> season
def singular(df):
    """ Está funcíon convierte en singular la palabra seasons
    Args: 
        df (str): Datafraem al cuál se le aplicara la transformación. 

    Returns: Dataframe transformado. 
    """
    df["duration_type"] = df["duration_type"].replace('seasons','season',regex=True)
    return df

#Ejecutar función singular
df_amazon =singular(df_amazon)
df_disney =singular(df_disney)
df_hulu = singular(df_hulu)
df_netflix= singular(df_netflix)

#En esta parte se unifican las diferentes tablas de datos
db = pd.concat([df_amazon,df_disney,df_hulu,df_netflix],axis=0)

#Se exporta el archivo a un csv. Está es la tabla que se ingesta para la busquedas
db.to_csv('./Datasets/dataET.csv')