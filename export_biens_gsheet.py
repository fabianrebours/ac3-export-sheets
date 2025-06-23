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
        "id",
        "customers_id",
        "price",
        "created_at",
        "last_modified",
        "model",
        "status_web",
        "[CT] Type de bien",
        "[CT] Code postal",
        "[CT] Ville",
        "[CT] Dans un rayon",
        "[CT] Géolocalisation de la recherche",
        "[CT] Statut du bien",
        "[CT] Type Mandat",
        "[CT] Pays",
        "[CT] Forme Mandat",
        "[CN] Honoraires Acquéreur",
        "Photos",
        "Rooms",
        "Description_title",
        "Description_text",
        "Customer_nom",
        "Customer_email",
        "Customer_tel",
        "Customer_creation_date",
        "Customer_next_contact",
        "Customer_last_action",
        "Category_name",
        "Themes",
        "INSEE_code_insee",
        "INSEE_commune",
        "INSEE_arrondissement",
        "INSEE_secteur",
        "Statistic_nb_vues",
        "Statistic_nb_contacts",
        "Statistic_nb_visites",
        "Suivi_par_nom",
        "Suivi_par_email",
        "Suivi_par_tel",
        "Suivi_par_mobile",
        "Cree_par_nom",
        "Cree_par_email",
        "Cree_par_tel",
        "Cree_par_mobile",
        "[CT] Type de transaction",
        "[CT] Cuisine",
        "[CT] Type Chauffage",
        "[CT] Date Echéance",
        "[CT] Etat général",
        "[CT] Mode Chauffage",
        "[CT] Etat intérieur",
        "[CT] Fenêtres",
        "[CT] Adresse",
        "[CT] Date Mandat",
        "[CT] Type de Stationnement",
        "[CT] Neuf - Ancien",
        "[CT] Isolation",
        "[CT] Points forts",
        "[CT] Exposition Séjour",
        "[CT] Etat Communs",
        "[CT] Consommation énergie primaire",
        "[CT] Gaz Effet de Serre",
        "[CT] Date établissement Diagnostic Energétique",
        "[CT] C.P. ou Ville internet",
        "[CT] Dans un rayon web",
        "[CT] N° lot",
        "[CT] Nb Lots Copropriété",
        "[CT] Procédures diligentées c/ syndicat de copropriété",
        "[CT] Titre Fiche Commerciale",
        "[CT] URL Prise de RDV en ligne",
        "[CN] Prix",
        "[CN] Nombre pièces",
        "[CN] Surface",
        "[CN] Surface terrain",
        "[CN] Chambres",
        "[CN] Etage",
        "[CN] Salle(s) d'eau",
        "[CN] Nombre places parking",
        "[CN] Cave(s)",
        "[CN] Année construction",
        "[CN] Nombre étages",
        "[CN] Prix Net Vendeur",
        "[CN] Accès Bus",
        "[CN] Accès Ecole",
        "[CN] Accès RER",
        "[CN] WC",
        "[CN] N° cave",
        "[CN] N° parking",
        "[CN] Honoraires Vendeur",
        "[CN] Valeur consommation énergie primaire",
        "[CN] Valeur Gaz Effet de serre",
        "[CN] Surface loi Carrez",
        "[CN] Charges annuelles (ALUR)",
        "[CN] Délai publication mandat",
        "[CN] Diagnostic de performance énergétique (10 ans)",
        "[CN] ERP - Etat des risques et pollution (6 mois)",
        "[CN] Amiante - pour les constructions avant le 01/07/1997 (Si négatif : sans limite de validité et si positif préconisations à suivre)",
        "[CN] Repérage présence termites - selon la zone",
        "[CN] Constat des risques d'Exposition au plomb pour les biens d'habitation construits avant le 01/01/1949 (illimité si négatif et 1 an si positif)",
        "[CN] Diagnostic nuisance sonore aérienne",
        "[CN] Etat de l'installation intérieure de gaz pour les installations intérieures de gaz de + de 15 ans (3 ans)",
        "[CN] Etat de l'installation intérieure d'électricité pour les installations intérieures d'électricité de + de 15 ans (3 ans)",
        "[CN] Mesurage Loi Carrez pour les copropriétés (sans limite de validité tant que de nouveaux travaux ne sont pas effectués)",
        "[CN] Diagnostic parasite",
        "[CN] Etat de l'installation d'assainissement - Collectif (préconisations à suivre en fonction du compte rendu)",
        "[CN] Etat de l'installation d'assainissement - non collectif (préconisations à suivre en fonction du compte rendu)",
        "[CN] Autres",
        "[CN] Titre de propriété (complet)",
        "[CN] Justificatif d'identité (copie carte d'identité, passeport)",
        "[CN] Justificatif de domicile (copie facture EDF,...)",
        "[CN] Livret de famille",
        "[CN] Taxe d'habitation",
        "[CN] Taxe foncière",
        "[CN] Facture de consommation d'énergie (EDF, GDF,fioul...)",
        "[CN] La liste du mobilier et matériels divers restant (hors cuisine aménagée et équipements intégrés)",
        "[CN] Facture et/ou contrat entretien chaudière (si chauffage individuel)",
        "[CN] Attestation ramonage",
        "[CN] Pouvoir(s) du représentant de la personne morale Vendeur (si mandant société ou association)",
        "[CN] Procuration du représentant de l'indivision (si les mandants sont en indivision)",
        "[CN] Extrait Kbis de moins de 3 mois",
        "[CN] Statuts de la société",
        "[CN] PV AG de la société",
        "[CN] Autorisation écrite du locataire pour visite",
        "[CN] Congé adressé par le propriétaire",
        "[CN] Congé adressé par le locataire",
        "[CN] Bail et avenants éventuels (si le bien est vendu loué)",
        "[CN] Attestation successorale (en cas de succession, donation, adjudication, etc ...)",
        "[CN] Extrait du POS ou du PLU",
        "[CN] Plan cadastral",
        "[CN] Attestation de déclaration de puits ou forage",
        "[CN] Attestation dommages / ouvrages (si résidence de - 10 ans et agrandissements ou travaux nécessitant des autorisations)",
        "[CN] Certificat de conformité (si résidence de - 10 ans et agrandissements ou travaux nécessitant des autorisations)",
        "[CN] Certificat de conformité du réseau collectif d'assainissement",
        "[CN] Contrat d'affichage",
        "[CN] Déclaration d'Achèvement des Travaux (DAT)",
        "[CN] Déclaration de Travaux (DT)",
        "[CN] Facture vidange des fosses septiques",
        "[CN] Permis de construire (si résidence de - 10 ans et agrandissements ou travaux nécessitant des autorisations)",
        "[CN] Plan Maison",
        "[CN] Règlement et cahier des charges du lotissement",
        "[CN] Assurances constructeurs / artisans (garanties décennales)",
        "[CN] Adresses constructeurs/artisans",
        "[CN] Règlement de lotissements",
        "[CN] PV d'assemblée générales (les 3 derniers)",
        "[CN] DTA et fiche synthétique",
        "[CN] Convocation et ordre du jour prochaine AG (si proche de l'AG annuelle et si convocation déjà reçue)",
        "[CN] Pré Etat daté",
        "[CN] PV d'Assemblée Générale Extraordinaire (si décisions urgentes de travaux)",
        "[CN] Règlement de copropriété et état descriptif de division ainsi que les actes les modifiant s'ils ont été publiés",
        "[CN] Appels de Fonds trimestriels (les 4 derniers)",
        "[CN] Décompte des charges de copropriété (dernier décompte approuvé)",
        "[CN] PV de réception de l'appartement (si résidence de - 10 ans)",
        "[CN] Carnet d'entretien de l'immeuble",
        "[CN] Certificat d'urbanisme",
        "[CN] Matrice cadastrale",
        "[CN] Document d'arpentage",
        "[CN] Déclaration préalable",
        "[CN] Copie du permis d'aménager",
        "[CN] Arrêt autorisant la vente anticipée des lots",
        "[CN] Déclaration d'achèvement des travaux",
        "[CN] Devis comprenant le coût global de la division",
        "[CN] Arrêté autorisant le lotissement",
        "[CN] Dépôt de pièces du lotissement auprès du notaire",
        "[CN] Autorisation de cession anticipée des lots",
        "[CN] Statuts de l'association syndicale",
        "[CN] Règlement de lotissement",
        "[CN] Cahier des charges",
        "[CN] Garantie financière accordée au lotisseur pour la finition des travaux",
        "[CN] Plan de masse",
        "[CN] Plan de division",
        "[CN] Plan des réseaux",
        "[CN] Justificatifs de revenus (Avis d'imposition, 3 derniers bulletins de salaire",
        "[CN] Justificatifs du prêt immobilier",
        "[CN] Justificatifs de l'épargne",
        "[CN] Loyer de base",
        "[CN] Cotisation fonds travaux",
        "[FT] Texte Fiche Commerciale",
        "[CT] N° Mandat",
        "[CT] Construction",
        "[CN] Loyer mensuel HC",
        "[CN] Taxe Foncière",
        "[CN] Coût Energie",
        "[CN] Prix Estimé (Maxi)",
        "[CN] Prix Estimé (Mini)",
        "[CT] Couverture",
        "[CT] Etat extérieur",
        "[CT] Date Estimation",
        "[CN] Provision sur charges",
        "[CN] Charges copropriété",
        "[CT] Surface divisible",
        "[CT] Méca. Chauffage",
        "[CT] Cheminée",
        "[CT] Assainissement",
        "[CT] Syndic",
        "[CT] Numéro cadastre",
        "[CT] Nom section cadastrale",
        "[CT] Année de référence des prix de l'énergie (DPE réalisés jusqu'au 30/06/2024)",
        "[CT] Forme Toiture",
        "[CT] Date d'établissement Etat des Risques et Pollutions(ERP)",
        "[CT] Visite virtuelle privée",
        "[CN] Salle(s) de bains",
        "[CN] Nombre garages/Box",
        "[CN] Surface Jardin",
        "[CN] Montant minimum estimé des dépenses annuelles d'énergie pour un usage standard",
        "[CN] Montant maximum estimé des dépenses annuelles d'énergie pour un usage standard",
        "[FT] Désignation du bien",
        "[FT] Lien vidéo",
        "[CN] Hauteur sous plafond",
        "[CN] Estimation du coût annuel des énergies pour un usage standard (DPE réalisé avant le 01/07/2021)",
        "[CN] Quote part de charges",
        "[CT] Porte",
        "[CT] N° Digicode",
        "[CT] Heures de Visite",
        "[CT] Nom Contact",
        "[CT] Téléphone Contact",
        "[CT] Points faibles",
        "[CT] Agences autorisées",
        "[CT] Date dernière assemblée",
        "[CN] Accès Métro",
        "[FT] URL origine pige",
        "[CT] Tantièmes",
        "[CN] Surface séjour",
        "[CN] Surface Cave",
        "[CT] Diagnostic Perf. Numérique",
        "[CN] Nombre niveaux",
        "[CN] Surface terrasse",
        "[CT] Mitoyenneté",
        "[CN] Surface au sol",
        "[CT] Standing",
        "[CT] Volets",
        "[CT] Type Bail",
        "[CN] Prix de départ",
        "[CN] Loyer charges comprises",
        "[CT] Date premier mandat",
        "[CT] Organisation Diagnostics",
        "[CT] Dont lots d'habitation",
        "[CT] Date établissement Audit Energétique",
        "[CT] Dernière mise à jour",
        "[CT] Date Diagnostic Elect",
        "[CT] Consommation énergie finale",
        "[CN] Surface de référence",
        "[CN] Valeur consommation énergie finale",
        "[CT] Référence Affaire",
        "[CT] Nom Résidence",
        "[FT] Consignes/Itinéraires",
        "Compromis_date_compromis",
        "Compromis_date_acte",
        "Compromis_date_offre",
        "Compromis_date_annulation",
        "Compromis_date_fin_sru",
        "Compromis_status",
    ]


    df = df.reindex(columns=column_order)

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
