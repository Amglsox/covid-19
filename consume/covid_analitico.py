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
from pyvirtualdisplay import Display
from selenium import webdriver
import re
import time
# In[]
### Definicao das variaveis de log
LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}
level_name = 'Covid19'
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level=level)
#In []
### get dados do endpoint do governo
def get_data_dim_regiao():
    try:
        logging.info("Get dados da API Covid 19")
        url = "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalSintese"
        response = requests.get(url)
        if response.status_code == 200:
            dados = response.json()
            return dados
    except Exception as ex:
        logging.error(ex)

# In []
### Metodo de web scraping
def get_data_atualizacao():
    display = Display(visible=0, size=(800, 600))
    display.start()
    url = 'https://covid.saude.gov.br/'
    pattern = "(\d{1,4}([.\-/])\d{1,2}([.\-/])\d{1,4}).(\d{2}(:)\d{2})"
    data = ""
    ##tenta com o browser do Firefox se tiver disponivel
    try:
        browser = webdriver.Firefox()
        browser.get(url)
        time.sleep(20)
        data = re.search(pattern, str(browser.page_source)).group(1)
        browser.quit()
    except Exception as e:
        print (e)
    ##tenta com o browser do chrome se estiver disponivel
    try:
        browser = webdriver.Chrome()
        browser.get(url)
        time.sleep(20)
        data = re.search(pattern, str(browser.page_source), re.IGNORECASE).group(1)
        browser.quit()
    except Exception as e:
        print (e)    
    finally:
        display.stop()
        return datetime.date(int(data[6:10]),int(data[3:5]), int(data[:2]))
# In[]
### GET DATA DE BANCO DADOS
def get_data_banco_dados():
    query = """SELECT MAX(DT_REFERENCIA) AS DT_REFERENCIA FROM fat_covid_new"""
    conn = get_engine_jdbc().connect()
    dt_bd = conn.execute(query).fetchall()[0][0]
    return dt_bd
# In[]:
### Define Schema de dados
def define_schema(dict_dados,dt_governo):
    try:
        logging.info("Define Schema")
        ### Formata o Json a nivel Estado
        ls_casos_obitos = list(map(lambda x:list(map(lambda y:{'nm_estado':y['_id'],
                                     'qt_obitos_confirmados':y['obitosAcumuladoNovos'],
                                     'qt_casos_confirmados':y['casosAcumuladoNovos'],
                                     'populacaoTCU2019': y['populacaoTCU2019'],
                                     'nm_regiao': x['_id'],
                                     'dt_insert': datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                                     'dt_referencia':dt_governo,
                                     'bt_ultima_carga':1
                                    },x['listaMunicipios'])),dict_dados[1:]))
        
        ### Remove da estrutura array aninhado
        lista_dicts = []
        for item_externo in ls_casos_obitos:
            for item_interno in item_externo:
                lista_dicts.append(item_interno)
        df = pd.DataFrame(data=lista_dicts, columns=['nm_estado','qt_obitos_confirmados','qt_casos_confirmados','nm_regiao','populacaoTCU2019','dt_insert','dt_referencia','bt_ultima_carga'])
        
        ### Adiciona dados do Brasil, sumarizado
        brasil = {'nm_estado':dict_dados[0]['_id'],
           'qt_obitos_confirmados':dict_dados[0]['obitosAcumuladoNovos'],
           'qt_casos_confirmados':dict_dados[0]['casosAcumuladoNovos'],
           'populacaoTCU2019': dict_dados[0]['populacaoTCU2019'],
           'nm_regiao': dict_dados[0]['_id'],
           'dt_insert': datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
           'dt_referencia':dt_governo,
           'bt_ultima_carga':1
          }
        df = df.append(brasil, ignore_index=True)
        return df
    except Exception as ex:
        logging.error(ex)

# In[]:
### CRIA ENGINE DE ESCRITA DE DADOS NO POSTGRESQL
def get_engine_jdbc():
    return create_engine(os.environ['DATABASE'])

def update_bt_ultimacarga():
    try:
        logging.info("Update Fat Covid Brasil")
        update = """update fat_covid_new set bt_ultima_carga = 0"""
        conn = get_engine_jdbc().connect()
        conn.execute(update)
    except Exception as ex:
        logging.error(ex)

# In[]:
### INSERT INTO JDBC
def write_jdbc(df):
    try:
        logging.info("Insert into Fat Covid Brasil")
        engine = get_engine_jdbc()
        df.to_sql('fat_covid_new', engine, index=False, if_exists='append', 
        dtype={"nm_estado": VARCHAR,
        "qt_casos_confirmados": NUMERIC,
        "qt_obitos_confirmados": NUMERIC,
        "nm_regiao": VARCHAR,
        "populacaoTCU2019": NUMERIC,
        "dt_insert": DATE,
        "dt_referencia":DATE,
        "bt_ultima_carga":INT
        })
    except Exception as ex:
        logging.error(ex)
# In[]:
# Save File Json do Dia
def write_file_json(df):
    path = os.path.join('./'+str(datetime.date.today().strftime('%Y'))+'/'+str(datetime.date.today().strftime('%m'))+'/'+str(datetime.date.today().strftime('%d'))+'/')
    filename = 'covid_19_'+str(datetime.datetime.today().strftime('%Y-%m-%d%Y-%m-%d'))
    os.makedirs(os.path.join(path), exist_ok=True)
    df.to_json(path_or_buf=path+filename,orient='records')
# In[]:
def main():
    dt_governo = get_data_atualizacao()
    dt_bd = get_data_banco_dados()
    print("Data Governo: ", dt_bd, "Data Banco de Dados:", dt_bd)
    if dt_bd < dt_governo:
        logging.info('Get dados regiao')
        dict_dados = get_data_dim_regiao()
        logging.info('Define Schema')
        df = define_schema(dict_dados, dt_governo)
        logging.info('Update carga')
        update_bt_ultimacarga()
        logging.info('Salvar arquivo json')
        write_file_json(df)
        logging.info('Salva na tabela')
        write_jdbc(df)
main()
