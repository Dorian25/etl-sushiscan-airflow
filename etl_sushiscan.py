# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 11:46:02 2022

@author: Dorian
"""

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
from bs4 import BeautifulSoup
import cloudscraper
# returns a CloudScraper instance
scraper = cloudscraper.create_scraper()

website_scan = {"sushi-scan": "https://sushiscan.su/manga/list-mode/"}


def format_date(t_d):
    """

    exemple : "janvier 17, 2022" -> "17/01/2022"

    :param t_d:
    :return:
    """
    dict_mois = {"janvier": "01",
                 "février": "02",
                 "mars": "03",
                 "avril": "04",
                 "mai": "05",
                 "juin": "06",
                 "juillet": "07",
                 "août": "08",
                 "septembre": "09",
                 "octobre": "10",
                 "novembre": "11",
                 "décembre": "12"}

    t_date = t_d.split(" ")
    d = t_date[1].replace(",", "")
    m = dict_mois[t_date[0]]
    y = t_date[2]
    if int(d) < 10:
        return datetime.strptime("/".join(["0"+d, m, y]), '%d/%m/%Y')
    else:
        return datetime.strptime("/".join([d, m, y]), '%d/%m/%Y')


def format_txt_info(txt):
    """

    :param txt: texte brut à reformater
    :return: un tuple (key, value)
    """
    step1 = txt.replace("\n", "").strip()
    if step1.startswith("Statut "):
        t = step1.split("Statut ")
        return 'Statut', t[1]
    elif step1.startswith("Type "):
        t = step1.split("Type ")
        return 'Type', t[1]
    elif step1.startswith("Année de Sortie "):
        t = step1.split("Année de Sortie ")
        if t[1] == "2917":
            return 'Année de Sortie', "2017"
        return 'Année de Sortie', t[1]
    elif step1.startswith("Auteur "):
        t = step1.split("Auteur ")
        return 'Auteur', t[1]
    elif step1.startswith("Dessinateur "):
        t = step1.split("Dessinateur ")
        return 'Dessinateur', t[1]
    elif step1.startswith("Posté par "):
        t = step1.split("Posté par ")
        return 'Posté par', t[1]
    elif step1.startswith("Prépublié dans "):
        t = step1.split("Prépublié dans ")
        return 'Prépublié dans', t[1]
    elif step1.startswith("Posté le "):
        t = step1.split("Posté le ")
        return 'Posté le', format_date(t[1])
    elif step1.startswith("Mis à jour le "):
        t = step1.split("Mis à jour le ")
        return 'Mis à jour le', format_date(t[1])
    else:
        return False


def get_all_series():
    """
    Web Scrapping du site Sushi-scan qui permet de récupérer la liste de séries disponibles sur le site Sushi-scan.

    :return: Une liste de tuples (url: String, titrel: String) correspondant aux séries disponibles sur le site de Sushi-scan.
    """
    html_website = BeautifulSoup(scraper.get(website_scan["sushi-scan"]).text, 'html.parser')

    div_list = html_website.find("div", {"class": "soralist"})
    all_series = div_list.findAll("a", {"class": "series"})
    all_series_txturl = [(a.text, a["href"]) for a in all_series]

    return all_series_txturl


def get_all_datas_of_serie(title_serie, url_serie):

    # data to get :
    # left info : statut, type, année de sortie, auteur, posté le, mis à jour le
    # right info : all volumes and all url, number of volume

    """
    {"titre" : title_serie,
     "synopsis" : synopsis
     "statut" : status,
     "auteur": auteur,
     "dessinateur": dessinateur,
     "annee" : annee,
     "type" : type,
     "volumes" : volumes,
     "chapitres" : chapitres,
     }
    """

    html_website = BeautifulSoup(scraper.get(url_serie).text, 'html.parser')

    div_left_info = html_website.find("div", {"class": "info-left"})
    div_right_info = html_website.find("div", {"class": "info-right"})

    # left info
    div_tsinfo_bixbox = div_left_info.find("div", {"class": "tsinfo bixbox"})
    all_div_info = div_tsinfo_bixbox.findAll("div", {"class": "imptdt"})

    synopsis = div_right_info.find("div", {"class": "entry-content entry-content-single"})

    serie = {"Titre": title_serie,
             "Synopsis": synopsis.text.replace("\n", "").replace(u'\xa0', u' '),
             "URL": url_serie}

    volumes = []
    chapitres = []

    for div_info in all_div_info:
        txt_info = div_info.text
        if format_txt_info(txt_info):
            k, v = format_txt_info(txt_info)
            serie[k] = v

    # right info
    div_chapterlist = div_right_info.find("div", {"id": "chapterlist"})
    all_a_chapters = div_chapterlist.findAll("a")

    for a_chapter in all_a_chapters:
        volume_name = a_chapter.find("span", {"class": "chapternum"}).text.strip()
        date_ajout = a_chapter.find("span", {"class": "chapterdate"}).text

        type_volume = ""
        num = ""

        """
        if not("Volume" in volume_name) and not("Chapitre" in volume_name):
            print(t, type_volume, numero, volume_name)
        ['https://sushiscan.su/aku', 'le', 'chasseur', 'maudit', 'volume', '2/'] volume 2 VolumZE
        ['https://sushiscan.su/berserk', 'official', 'guidebook/'] official guidebook Guidebook
        ['https://sushiscan.su/ex', 'arm', 'volume', '5/'] volume 5 Voluem 5
        ['https://sushiscan.su/hajime', 'no', 'ippo', 'chapitre', '1382/'] chapitre 1382 Chapitr 1382
        ['https://sushiscan.su/higanjima', 'lile', 'des', 'vampires', 'le', 'destin', 'du', 'grand', 'frere/'] grand frere Hors-Série
        ['https://sushiscan.su/junji', 'ito', 'collection', 'english', 'edition', 'deluxe', 'tomie/'] deluxe tomie Tomié
        ['https://sushiscan.su/junji', 'ito', 'collection', 'english', 'edition', 'deluxe', 'gyo/'] deluxe gyo Gyo
        ['https://sushiscan.su/junji', 'ito', 'collection', 'english', 'edition', 'deluxe', 'uzumaki', 'spirale/'] uzumaki spirale Uzumaki/Spirale
        ['https://sushiscan.su/nami', 'vs', 'kalifa', 'one', 'shot/'] one shot One Shot
        ['https://sushiscan.su/one', 'piece', 'party', 'volume', '5/'] volume 5 Voluem 5
        ['https://sushiscan.su/one', 'punch', 'man', 'chapitre', '157/'] chapitre 157 Chapître 157
        ['https://sushiscan.su/the', 'boys', 'edition', 'deluxe', 'hs/'] deluxe hs Hors-Série                
        """
        if volume_name.lower().startswith("vol"):
            if len(volume_name.split(" ")) > 1:
                type_volume = "volume"
                num = volume_name.split(" ")[1]
            elif volume_name.lower() == "volumze":
                type_volume = "volume"
                num = "2"
            else:
                print(volume_name, "//", title_serie)
                type_volume = "volume"
            volumes.append({"Type": type_volume,
                            "Numéro": num,
                            "Posté le": format_date(date_ajout),
                            "URL": a_chapter["href"]})
        elif volume_name.lower().startswith("chap"):
            type_volume = "chapitre"
            num = volume_name.split(" ")[1]
            chapitres.append({"Type": type_volume,
                              "Numéro": num,
                              "Posté le": format_date(date_ajout),
                              "URL": a_chapter["href"]})
        """
        else:
            type_volume = "autre"
            autres.append({"Type": type_volume,
                            "Numéro": num,
                            "Posté le": format_date(date_ajout),
                            "URL": a_chapter["href"]})
        """

    serie["Volumes"] = volumes
    serie["Chapitres"] = chapitres

    return serie


def extract():
    all_series = get_all_series()
    docs = {"rows": []}

    for title_serie, url_serie in all_series:
        serie = get_all_datas_of_serie(title_serie, url_serie)
        docs["rows"].append(serie)

    print(len(all_series), "series disponibles !")
    with open('sushiscan_extract.json', 'w') as f:
        json.dump(docs, f, default=str)


def load():
    mongohook = MongoHook(conn_id="mongo_getmangas")
    mongohook.delete_many(mongo_collection="sushiscan",
                     mongo_db="getmanga_db",
                     filter_doc={})

    with open('sushiscan_extract.json', 'r') as f:
        data = json.load(f)

    mongohook.insert_many(mongo_collection="sushiscan",
                          mongo_db="getmanga_db",
                          docs=data["rows"])


DAG_DEFAULT_ARGS = {
    'owner': 'admin',
    'start_date': datetime(2022, 10, 2),
    'schedule_interval': '@daily'
}


with DAG(dag_id='etl_sushiscan',
         default_args=DAG_DEFAULT_ARGS,
         description='ETL from sushiscan.su') as dag:

    extract = PythonOperator(task_id="Extract",
                             python_callable=extract)

    load = PythonOperator(task_id="Load",
                          python_callable=load)

    extract >> load
