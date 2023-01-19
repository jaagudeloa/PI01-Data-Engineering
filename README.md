# <h1 align=center>**PROYECTO INDIVIDUAL Nº1**

# <h1 align=center>**`Data Engineering`**</h1>  

Éste trabajo hace parte de la formación práctica del bootcamp de Data Science de la academia Henry y corresponde a mí primer proyecto individual. 

# <h2> **Objetivos**</h1>
- Realizar una extracción de datos desde diversas fuentes. 
- Aplicar ciertas transformaciones y/o ejecutar la limpieza de datos
- Poner a disposición los datos mediante la elaboración y ejecución de una API 

***
## **Tareas a Ejecutar**

1.ETL (Extraction, Transform, Load). Cómo paso final se creó una tabla relacionando y unificando el conjunto de datos

2.Crear una API con FastAPI 

3.Realizar las consultas

4.Realizar un deployment en Deta

***
## **Tecnologías utilizadas**
El uso de las siguientes tecnologías permitirá llevar a cabo las tareas propuestas

- Python:   
    - Pandas
    - pymysql
- FastAPI
- Uvicorn
- Deta

***
## **Archivos del repositorio**
- **`Datasets`**: (./Datasets/) En esta carpeta se encuentran los archivos de los datos fuente utilizados para realizar el proyecto, y también el archivo que se creó con los resultados del ETL.
Las plataformas a las cuáles pertenecen los datos son: amazon, netflix,disney,hulu 
- **`ETL`**: (./ETL.py/) Script para realizar el ETL de los datos.
- **`main.py`**: (/main.py) Script para instanciar la API, con las funciones de consultas.

***
## `1.ETL(Extraction, Transform, Load)`
Con el archivo ETL.py se realizaron las tareas de extracción de datos desde archivos tipo(csv), convirtiéndolos a distintos dataframes. Sobre los Dataframes y haciendo uso de pandas se realizaron las tareas de transformación encomendadas que se resumían en: 

+ Generar campo **`id`**: Cada id se compondrá de la primera letra del nombre de la plataforma, seguido del show_id ya presente en los datasets (ejemplo para títulos de Amazon = **`as123`**)

+ Los valores nulos del campo rating deberán reemplazarse por el string “**`G`**” (corresponde al maturity rating: “general for all audiences”

+ De haber fechas, deberán tener el formato **`AAAA-mm-dd`**

+ Los campos de texto deberán estar en **minúsculas**, sin excepciones

+ El campo ***duration*** debe convertirse en dos campos: **`duration_int`** y **`duration_type`**. El primero será un integer y el segundo un string indicando la unidad de medición de duración: min (minutos) o season (temporadas)

Como paso final se unificaron las tablas, generando una única tabla que posteriormente se utilizará en la ejecución de las consultas

***
## `2.Crear una API con FastAPI`
Para la creación de la API, se utilizó el archivo main.py, en donde se configuraron las funciones para la realización de consultas. El script instancia la API que carga el CSV ya transformado para realizar dichas consultas, y devuelve los resultados esperados.
***
## `3. Generación de Consultas`
Se decidió como primer paso, ejecutar las consultas de manera local para verificar su buen funcionamiento. Para ello se usó la herramienta uvicorn. Para la implementación de las consultas se uso la biblioteca python **pandasql** :

A continuación se ejemplifica una busqueda con el uso de pandasql:

```
def get_word_count(plataforma:str,keyword:str):
dic_plataforma ={"netflix":"n","amazon":"a","disney":"d","hulu":"h"}
id_value = dic_plataforma[plataforma]+"%" keyword="%"+keyword+"%"
consulta1 = f"SELECT COUNT(title) FROM db WHERE title LIKE '{keyword}' AND id LIKE '{id_value}' "
return (plataforma,sqldf(consulta1).to_string(index=False,header=False))
```


Para este proyecto, se solicitaban las siguientes consultas:

+ ### `Cantidad de veces que aparece una keyword en el título de películas/series, por plataforma`  

    El request debe ser:
    
    ```
    /get_word_count/plataforma,keyword
    ```
    Un ejemplo de ello es:
    ```
    /get_word_count/netflix,love
    ```


+ ### `Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año`   
  
    El request debe ser:
    ```
    /get_score_count/plataforma,puntaje,año
    ```
    Un ejemplo de ello es:
    ```
    /get_score_count/netflix,85,2010
    ``` 
  
+ ### `La segunda película con mayor score para una plataforma determinada según el orden alfabético de los títulos` 
  
    El request debe ser:
    ```
    /get_second_score/plataforma
    ```
    Un ejemplo de ello es:
    ```
    /get_second_score/amazon
    ```    

+ ### `Película que más duró según año, plataforma y tipo de duración` 

    El request debe ser:
    ```
    /get_longest/plataforma,tipo de duración,año
    ```
    Un ejemplo de ello es:
    ```
    /get_longest/netflix,min,2016
    ```    
+ ### `Cantidad de series y películas por rating`

    El request debe ser:
    ```
    /get_rating_count/rating
    ```
    Un ejemplo de ello es:
    ```
    /get_rating_count/18+
    ```    
***
## `4.Realizar un deployment en Deta`
Para ejecutar el (deployment) de la aplicación se hizo uso de la plataforma DETA (que no necesita dockerización).
Allí se generó el siguiente Endpoint desde donde se ejecutarán las consultas previstas:

- Endpoint: 

    ## https://xkqak4.deta.dev/   

- Ejemplo Consulta : 
   
   ## https://xkqak4.deta.dev/get_word_count/netflix,love

***
Con los pasos anteriormente ejecutados se logró con éxito cumplir con los requerimientos del ejercicio propuesto