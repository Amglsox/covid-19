
# coding: utf-8

# In[255]:
from sqlalchemy.types import *
import requests
import datetime
import pandas as pd
import sys
import os
from sqlalchemy import create_engine
import logging

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}
level_name = 'Covid19'
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level=level)


# In[256]:


def get_data_dim_regiao():
    try:
        logging.info("Get dados da API Covid 19")
        url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalRegiaoUf"
        response = requests.get(url)
        if response.status_code == 200:
            dados = response.json()
            return dados
    except Exception as ex:
        logging.error(ex)


# In[257]:


def define_schema(dict_dados, list_regiao, lista_par_valor):
    dados_curated = []
    try:
        logging.info("Define Schema")
        for regiao in list_regiao:
                for par in lista_par_valor:
                    for item in par[regiao]:
                        dados_curated += list(map(lambda x: {'dt_referencia':datetime.date(2020,int(x['_id'][3:5]),int(x['_id'][0:2])).strftime('%Y-%m-%d'),
                            'qt_casos_confirmados': float(x['casosAcumulado']),
                            'qt_obitos_confirmados': float(x['obitosAcumulado']),
                            'nm_estado': item,
                            'nm_regiao': regiao,
                            'dt_insert': datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                           }, dict_dados[regiao][item]['dias']))
        return dados_curated
    except Exception as ex:
        logging.error(ex)


# In[258]:


### CRIA ENGINE DE ESCRITA DE DADOS NO POSTGRESQL
def get_engine_jdbc():
    return create_engine('postgresql+psycopg2://username:password@host:5432/database')

### INSERT INTO JDBC
def write_jdbc(df):
    try:
        logging.info("Insert into Fat Covid Brasil")
        engine = get_engine_jdbc()
        df.to_sql('fat_covid_oficial', engine, index=False, if_exists='replace', 
        dtype={"nm_regiao": VARCHAR,
        "dt_referencia": DATE,
        "qt_casos_confirmados": NUMERIC,
        "qt_obitos_confirmados": NUMERIC,
        "nm_estado": VARCHAR,
        "dt_insert": DATE
        })
    except Exception as ex:
        logging.error(ex)


# In[259]:


def main():
    dict_dados = get_data_dim_regiao()
    list_regiao = list(dict_dados.keys())[3:]
    lista_par_valor = [{x: list(dict_dados[x].keys()) for x in list(dict_dados.keys())[3:]}]
    write_jdbc(pd.DataFrame(data=define_schema(dict_dados, list_regiao, lista_par_valor)))

main()