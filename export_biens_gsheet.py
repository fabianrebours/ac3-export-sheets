#!/usr/bin/env python3
# export_biens_to_gsheet.py

import os
import requests
import json
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from tqdm import tqdm
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dateutil.parser import parse

# Paramètres
SITE_ID = "148899"
BASE_URL = "https://v2.immo-facile.com/api"
GOOGLE_SHEET_NAME = "AC3 reporting"
GOOGLE_SHEET_TAB = "Biens"
SPREADSHEET_ID = "1TJQ7WZGelLXzLqq6RGbZbD5BIyTaIX4XkCG9Wv_JmFM"

# Fonction de formatage des dates
def format_date(date_str):
    if not date_str:
        return ""
    try:
        dt = parse(date_str)
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return date_str

# 1️⃣ Récupérer un token
def get_access_token():
    url = f"{BASE_URL}/client/token/site?site_id={SITE_ID}"

    auth = HTTPBasicAuth("123", "CiH3q4GdgJDsVueYBls9VC2ph2JfiWcK6JUDVIX3isC6MGcvauLdeDslL4Jc845l")

    response = requests.post(url, auth=auth)
    response.raise_for_status()
    token = response.json()["access_token"]
    print("✅ Token récupéré")
    return token

# 2️⃣ Appeler /site/products/search
def fetch_products(token):
    url = f"{BASE_URL}/site/products/search"
    params = {
        "fetch": "criteres_text,suivi_par,cree_par,criteres_number,criteres_fulltext,criteres_flag,publications,products_photos,customer,actions_history,criteres_publication_errors,compromis,descriptions,rooms,statistic,insee,scopes,category,themes",
        "count": 100
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {}  # on ne filtre pas, on veut tout
    response = requests.post(url, params=params, headers=headers, json=body)
    response.raise_for_status()
    print("✅ Produits récupérés")
    return response.json()

# 3️⃣ Transformer en DataFrame plat
def flatten_products(products):
    print("📦 Flatten des produits...")
    rows = []
    for prod in tqdm(products):
        row = {
            "id": prod.get("id"),
            "customers_id": prod.get("customers_id"),
            "price": prod.get("price"),
            "created_at": format_date(prod.get("created_at")),
            "last_modified": format_date(prod.get("last_modified")),
            "model": prod.get("model"),
            "status_web": prod.get("status_web"),
        }

        # suivi_par
        suivi = prod.get("suivi_par")
        if suivi:
            row["Suivi_par_nom"] = f"{suivi.get('firstname', '')} {suivi.get('lastname', '')}"
            row["Suivi_par_email"] = suivi.get("email")
            row["Suivi_par_tel"] = suivi.get("phone")
            row["Suivi_par_mobile"] = suivi.get("mobile_phone")

        # cree_par
        cree = prod.get("cree_par")
        if cree:
            row["Cree_par_nom"] = f"{cree.get('firstname', '')} {cree.get('lastname', '')}"
            row["Cree_par_email"] = cree.get("email")
            row["Cree_par_tel"] = cree.get("phone")
            row["Cree_par_mobile"] = cree.get("mobile_phone")

        # criteres_text
        for critere in prod.get("criteres_text", []):
            key = critere.get("critere_name")
            value = critere.get("critere_value")
            row[f"[CT] {key}"] = value

        # criteres_number
        for critere in prod.get("criteres_number", []):
            key = critere.get("critere_name")
            value = critere.get("critere_value")
            row[f"[CN] {key}"] = value

        # criteres_fulltext
        for critere in prod.get("criteres_fulltext", []):
            key = critere.get("critere_name")
            value = critere.get("critere_value")
            row[f"[FT] {key}"] = value

        # products_photos
        photos = prod.get("products_photos", [])
        photo_urls = [photo.get("chemin") for photo in photos if photo.get("chemin")]
        row["Photos"] = "; ".join(photo_urls)

        # rooms
        rooms = prod.get("rooms", [])
        rooms_desc = []
        for room in rooms:
            room_type = room.get("type_piece", "Inconnu")
            surface = room.get("surface_piece", "NA")
            rooms_desc.append(f"{room_type} ({surface} m²)")
        row["Rooms"] = "; ".join(rooms_desc)

        # compromis
        compromis_list = prod.get("compromis", [])
        if compromis_list:
            comp = compromis_list[0]
            row["Compromis_date_compromis"] = format_date(comp.get("date_compromis"))
            row["Compromis_date_acte"] = format_date(comp.get("date_acte"))
            row["Compromis_date_offre"] = format_date(comp.get("date_offre"))
            row["Compromis_date_annulation"] = format_date(comp.get("date_annulation"))
            row["Compromis_date_fin_sru"] = format_date(comp.get("date_fin_sru"))
            row["Compromis_status"] = comp.get("status", {}).get("text")

        # descriptions
        desc_list = prod.get("descriptions", [])
        if desc_list:
            desc = desc_list[0]
            row["Description_title"] = desc.get("title")
            row["Description_text"] = desc.get("description")

        # customer
        customer = prod.get("customer")
        if customer:
            row["Customer_nom"] = f"{customer.get('firstname', '')} {customer.get('lastname', '')}"
            row["Customer_email"] = customer.get("email")
            row["Customer_tel"] = customer.get("phone")
            row["Customer_creation_date"] = format_date(customer.get("creation_date"))
            row["Customer_next_contact"] = format_date(customer.get("next_contact"))
            row["Customer_last_action"] = format_date(customer.get("last_action"))

        # category
        category = prod.get("category")
        if category:
            row["Category_name"] = category.get("name")

        # themes
        themes = prod.get("themes", [])
        row["Themes"] = "; ".join([theme.get("theme_name") for theme in themes if theme.get("theme_name")])

        # insee
        insee = prod.get("insee")
        if isinstance(insee, dict):
            row["INSEE_code_insee"] = insee.get("code_insee")
            row["INSEE_commune"] = insee.get("commune")
            row["INSEE_arrondissement"] = insee.get("arrondissement")
            row["INSEE_secteur"] = insee.get("secteur")
        else:
            row["INSEE_code_insee"] = None
            row["INSEE_commune"] = None
            row["INSEE_arrondissement"] = None
            row["INSEE_secteur"] = None

        # statistic
        statistic = prod.get("statistic")
        if statistic:
            row["Statistic_nb_vues"] = statistic.get("nb_vues")
            row["Statistic_nb_contacts"] = statistic.get("nb_contacts")
            row["Statistic_nb_visites"] = statistic.get("nb_visites")

        rows.append(row)

    df = pd.DataFrame(rows)
    print(f"✅ {len(df)} lignes construites")
    return df

# 4️⃣ Envoyer vers Google Sheets
def upload_to_google_sheets(df):
    print("📤 Upload vers Google Sheets...")

    # Authentification
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    cred_path = "/etc/secrets/credentials.json"
    credentials = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(credentials)

    # Ouvre le Google Sheet
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(GOOGLE_SHEET_TAB)

    # ⚠️ On NE met plus rien en A1 dans "Biens"
    # On vide la feuille (tout)
    worksheet.clear()

    # Upload du dataframe à partir de A1
    set_with_dataframe(worksheet, df, row=1, include_column_header=True, resize=True)

    # 👉 Date d’export dans une feuille séparée "Meta"
    export_date_str = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    try:
        meta_ws = sh.worksheet("Meta")
    except gspread.exceptions.WorksheetNotFound:
        meta_ws = sh.add_worksheet(title="Meta", rows=10, cols=2)

    meta_ws.update("A1", [["Dernière date d'export"], [export_date_str]])
    print(f"🕒 Export enregistré dans l'onglet 'Meta' : {export_date_str}")
    print("✅ Données envoyées vers Google Sheets")

# MAIN
if __name__ == "__main__":
    print("🚀 Démarrage export Biens vers Google Sheets...")
    token = get_access_token()
    products = fetch_products(token)
    df = flatten_products(products)
    upload_to_google_sheets(df)
    print("🎉 Export terminé !")
