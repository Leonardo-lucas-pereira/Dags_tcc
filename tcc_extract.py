import re
import bson
import spacy
import string
import logging
import pendulum

from datetime import datetime
from pymongo import MongoClient
from airflow.decorators import dag, task



logging.info('Hello')


# Variáveis --
pln = spacy.load("pt_core_news_sm")
stop_words = spacy.lang.pt.stop_words.STOP_WORDS

constring = 'mongodb://leonardo:senha123@localhost:27017'
client = MongoClient(constring)
db = client['diario']


userPosts = db.userPosts
userPostProcessed = db.userPostProcessed


start = datetime(2022, 10, 2)
end = datetime(2022, 10, 19)
myquery = { "created": {'$gte': start } }


def preprocessamento(texto):
    # Letras minúsculas
    texto = texto.lower()

    # Espaços em branco
    texto = re.sub(r" +", ' ', texto)

    # Lematização
    documento = pln(texto)

    lista = []
    for token in documento:
        lista.append(token.lemma_)
    # Stop words e pontuações
    lista = [palavra for palavra in lista if palavra not in stop_words and palavra not in string.punctuation]
    lista = ' '.join([str(elemento) for elemento in lista if not elemento.isdigit()])

    return lista


@task()
def ExtractPagesMongo(post, ds=None, ds_nodash=None):
    page_post = {}
    page_post['_id'] =  bson.objectid.ObjectId()
    page_post['id_user'] = post['id_user']
    page_post['content'] = preprocessamento(post['content'].split(":")[1])
    page_post['created'] = datetime.today()
    post_id = userPostProcessed.insert_one(page_post).inserted_id
    print(post_id)


@dag(
    schedule_interval = "0 0 * * 2-6",
    start_date = pendulum.datetime(2022,10,16,tz="UTC"),
    catchup = True
)
def MongoInsert():
    for post in userPosts.find(myquery).sort("id_user"):
        name_task = f"Extract_Page_User_{post['id_user']}"
        ExtractPagesMongo.override(task_id=name_task)(post)


dag = MongoInsert()
