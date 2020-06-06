
# coding: utf-8

# In[1]:
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


# In[5]:


def get_data_sintese():
    try:
        logging.info("Get dados da API Covid 19")
        url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalSintese"
        response = requests.get(url)
        if response.status_code == 200:
            dados = response.json()
            return dados
    except Exception as ex:
        logging.error(ex)


# In[21]:


def define_schema(dict_dados):
    dados_curated = []
    try:
        logging.info("Define Schema")
        data = {
            'pais':dict_dados['regiao'],
            'dt_referencia':dict_dados['data'],
            'qt_casos_confirmados':dict_dados['casosAcumulado'],
            'qt_obitos_confirmados':dict_dados['obitosAcumulado'],
            'qt_recuperados':dict_dados['Recuperadosnovos'],
            'qt_acompanhamento':dict_dados['emAcompanhamentoNovos'],
            'dt_insert':datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}
        return data
    except Exception as ex:
        logging.error(ex)


# In[23]:


### CRIA ENGINE DE ESCRITA DE DADOS NO POSTGRESQL
def get_engine_jdbc():
    return create_engine('postgresql+psycopg2://username:pass@host:5432/database')

### INSERT INTO JDBC
def write_jdbc(df):
    try:
        logging.info("Insert into Fat Covid Brasil")
        engine = get_engine_jdbc()
        df.to_sql('fat_covid_agregado', engine, index=False, if_exists='replace', 
        dtype={"pais": VARCHAR,
        "dt_referencia": DATE,
        "qt_casos_confirmados": NUMERIC,
        "qt_obitos_confirmados": NUMERIC,
        "qt_acompanhamento": NUMERIC,
        "qt_recuperados": NUMERIC,
        "dt_insert": DATE
        })
    except Exception as ex:
        logging.error(ex)


# In[24]:


def main():
    dados = get_data_sintese()[0]
    dados_curated = define_schema(dados)
    df = pd.DataFrame(dados_curated, index=[0])
    write_jdbc(df)


# In[20]:
main()


