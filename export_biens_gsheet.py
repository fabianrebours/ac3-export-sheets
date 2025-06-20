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

        suivi = prod.get("suivi_par")
        if suivi:
            row["Suivi_par_nom"] = f"{suivi.get('firstname', '')} {suivi.get('lastname', '')}"
            row["Suivi_par_email"] = suivi.get("email")
            row["Suivi_par_tel"] = suivi.get("phone")
            row["Suivi_par_mobile"] = suivi.get("mobile_phone")

        cree = prod.get("cree_par")
        if cree:
            row["Cree_par_nom"] = f"{cree.get('firstname', '')} {cree.get('lastname', '')}"
            row["Cree_par_email"] = cree.get("email")
            row["Cree_par_tel"] = cree.get("phone")
            row["Cree_par_mobile"] = cree.get("mobile_phone")

        for critere in prod.get("criteres_text", []):
            row[f"[CT] {critere.get('critere_name')}"] = critere.get("critere_value")

        for critere in prod.get("criteres_number", []):
            row[f"[CN] {critere.get('critere_name')}"] = critere.get("critere_value")

        for critere in prod.get("criteres_fulltext", []):
            row[f"[FT] {critere.get('critere_name')}"] = critere.get("critere_value")

        photos = prod.get("products_photos", [])
        row["Photos"] = "; ".join([p.get("chemin") for p in photos if p.get("chemin")])

        rooms_desc = []
        for room in prod.get("rooms", []):
            rooms_desc.append(f"{room.get('type_piece', 'Inconnu')} ({room.get('surface_piece', 'NA')} m²)")
        row["Rooms"] = "; ".join(rooms_desc)

        compromis_list = prod.get("compromis", [])
        if compromis_list:
            comp = compromis_list[0]
            row["Compromis_date_compromis"] = format_date(comp.get("date_compromis"))
            row["Compromis_date_acte"] = format_date(comp.get("date_acte"))
            row["Compromis_date_offre"] = format_date(comp.get("date_offre"))
            row["Compromis_date_annulation"] = format_date(comp.get("date_annulation"))
            row["Compromis_date_fin_sru"] = format_date(comp.get("date_fin_sru"))
            row["Compromis_status"] = comp.get("status", {}).get("text")

        desc_list = prod.get("descriptions", [])
        if desc_list:
            row["Description_title"] = desc_list[0].get("title")
            row["Description_text"] = desc_list[0].get("description")

        customer = prod.get("customer")
        if customer:
            row["Customer_nom"] = f"{customer.get('firstname', '')} {customer.get('lastname', '')}"
            row["Customer_email"] = customer.get("email")
            row["Customer_tel"] = customer.get("phone")
            row["Customer_creation_date"] = format_date(customer.get("creation_date"))
            row["Customer_next_contact"] = format_date(customer.get("next_contact"))
            row["Customer_last_action"] = format_date(customer.get("last_action"))

        category = prod.get("category")
        if category:
            row["Category_name"] = category.get("name")

        row["Themes"] = "; ".join([t.get("theme_name") for t in prod.get("themes", []) if t.get("theme_name")])

        insee = prod.get("insee", {})
        if isinstance(insee, dict):
            row["INSEE_code_insee"] = insee.get("code_insee")
            row["INSEE_commune"] = insee.get("commune")
            row["INSEE_arrondissement"] = insee.get("arrondissement")
            row["INSEE_secteur"] = insee.get("secteur")

        statistic = prod.get("statistic")
        if statistic:
            row["Statistic_nb_vues"] = statistic.get("nb_vues")
            row["Statistic_nb_contacts"] = statistic.get("nb_contacts")
            row["Statistic_nb_visites"] = statistic.get("nb_visites")

        rows.append(row)

    df = pd.DataFrame(rows)
    print(f"✅ {len(df)} lignes construites")

    # 🔒 Ordre stable des colonnes
    # Liste à adapter si besoin — tronquée ici pour lisibilité
    column_order = [
        "id", "customers_id", "price", "created_at", "last_modified", "model", "status_web",
        "Suivi_par_nom", "Suivi_par_email", "Suivi_par_tel", "Suivi_par_mobile",
        "Cree_par_nom", "Cree_par_email", "Cree_par_tel", "Cree_par_mobile",
        "Photos", "Rooms", "Description_title", "Description_text",
        "Customer_nom", "Customer_email", "Customer_tel", "Customer_creation_date",
        "Customer_next_contact", "Customer_last_action", "Category_name", "Themes",
        "INSEE_code_insee", "INSEE_commune", "INSEE_arrondissement", "INSEE_secteur",
        "Statistic_nb_vues", "Statistic_nb_contacts", "Statistic_nb_visites",
        "Compromis_date_compromis", "Compromis_date_acte", "Compromis_date_offre",
        "Compromis_date_annulation", "Compromis_date_fin_sru", "Compromis_status"
    ]

    # Ajouter toutes les autres colonnes dynamiques
    other_cols = [col for col in df.columns if col not in column_order]
    df = df.reindex(columns=column_order + sorted(other_cols))

    return df


# 4️⃣ Envoyer vers Google Sheets
def upload_to_google_sheets(df):
    print("📤 Upload vers Google Sheets...")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    cred_path = "/etc/secrets/credentials.json"
    credentials = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(GOOGLE_SHEET_TAB)

    # ✅ Ne supprime pas tout : garde l'en-tête (ligne 1)
    worksheet.resize(rows=1)

    # 📝 Réécrit proprement les données (en-têtes + lignes)
    set_with_dataframe(worksheet, df, row=1, include_column_header=True, resize=True)

    # 🕒 Date dans "Meta"
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
