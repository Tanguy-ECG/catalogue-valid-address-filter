import re
import numpy as np
import pandas as pd

from unidecode import unidecode

### Récupération du fichier ainsi que le chemin qui mène au fichier.

brand = input("HV ou TOHAPI ?")

path = brand + "/"
print("Mettre un fichier excel nommée catalogues.xlsx avec les feuilles '{}_DEMANDEURS' et '{}_SCORING' dans le dossier '{}'".format(brand, brand, brand))

print("Lecture du fichier.")
### Lecture des fichiers
# Importation de la base des scorings
df_init = pd.read_excel(
    path + "catalogues.xlsx",
    sheet_name="{}_SCORING".format(brand),
    dtype={"postal_code": str}
).query(
    "country_cd == 'FR'" # On filtre que sur les français
    ).drop_duplicates( # On retire les doublons par rapport au mail
        subset=['email']
        ).map(lambda x: str(x).replace(";", " "))

# Importation de la base des demandeurs
df_demandeurs = pd.read_excel(
    path + "catalogues.xlsx",
    sheet_name="{}_DEMANDEURS".format(brand),
    usecols=df_init.columns.drop(["score", "postal_locality", "main_partner"]),
    dtype={"address_1": str}
).fillna("")

# Importation de la base des codes postaux
df_codes_postaux = pd.read_csv(
    "DATA" + "/" + "base_codes_postaux.csv",
     encoding='latin-1', sep = ";",
    usecols = ["Nom_de_la_commune", "Code_postal", "Ligne_5"],
    dtype={"Code_postal": str}
).sort_values("Ligne_5").drop_duplicates(["Nom_de_la_commune", "Code_postal"])

# Mettre sous bon format les accents
df_init["prenom"] = df_init["prenom"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')
df_init["nom"] = df_init["nom"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')
df_init["city"] = df_init["city"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')
df_init["street"] = df_init["street"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')
df_init["address_1"] = df_init["address_1"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')
df_init["address_2"] = df_init["address_2"].str.encode('latin-1', errors='ignore').str.decode('utf-8', errors='ignore')

## Verification que df_demandeurs et df_final ont les même noms de colonnes
if not list(df_demandeurs.columns) in list(df_init.columns):
    print("Le fichier des demandeurs n'a pas les même colonne que celui du scoring")

print("Début du process de nettoyage de la base.")
### Nettoyage de la base de données
df_clean = df_init.drop(
            "main_partner", axis=1
        )

## city
# Supprimer les caractères non alphabétiques avant la première lettre dans la colonne 'city'
df_clean['city'] = df_clean['city'].replace(r'^[^a-zA-Z-]*', '', regex=True)

# Supprimer ce qui n'est pas une lettre, un espace ou un tiret dans la colonne 'city'
df_clean['city'] = df_clean['city'].str.replace(r'[^a-zA-Z -]+', '', regex=True).replace("-", " ").str.strip()

# Supprimer les doubles espaces dans citu
df_clean['city'] = df_clean['city'].str.replace("  ", " ")

## street
# Retirer les "\t"
df_clean['street'] = df_clean['street'].str.replace("\t", "")

# Remplacer les [&, é, ",', (,-,è,_,ç,à] avec les numéros correspondants
correspondance = {
    '&&&': '111',
    '&&': '11',
    '&': '1',
    'ééé': '222',
    'éé': '22',
    'é': '2',
    '"""': '333',
    '""': '33',
    '"': '3',
    "'''": '444',
    "''": '44',
    "'": '4',
    '((((': '555',
    '(((': '55',
    '(': '5',
    '---': '666',
    '--': '66',
    '-': '6',
    'èèè': '777',
    'èè': '77',
    'è': '7',
    '___': '888',
    '__': '88',
    '_': '8',
    'ççç': '999',
    'çç': '99',
    'ç': '9',
    'ààà': '000',
    'àà': '00',
    'à': '0'
}

df_clean['street'] = df_clean['street'].str.replace(
    pat=r'([&,é,"\',\(\-è_çà]{1,3})\s+(rue|avenue)',
    repl=lambda match: correspondance.get(match.group(1), match.group(1)) + " " + match.group(2),
    flags=re.IGNORECASE,
    regex=True
)

# Mettre tout sans accent
df_clean["street"] = df_clean["street"].apply(lambda x: unidecode(x)).str.strip()
# Supprimer tout ce qui n'est pas un chiffre ou une lettre avant la première lettre ou chiffre de la chaine, en ignorant la casse.
df_clean['street'] = df_clean['street'].str.extract(r'([0-9a-zA-Z].*)', flags=re.IGNORECASE)[0]

## postal_code
# Supprimer tous les caractères non numériques de la colonne 'postal_code'.
df_clean['postal_code'] = df_clean['postal_code'].replace('[^0-9]', '', regex=True).apply(
    lambda x: '0' + x if len(x)==4 else x
)

print("Début du process de filtrage de la base.")
### Filtrage de la base de données

## postal_code
# Supprimer les codes postaux qui n'ont pas une taille de 5
df_filtrage = df_clean[df_clean["postal_code"].apply(lambda x: len(str(x).strip()) == 5)]

## city
# Supprimer les villes vides (suite au nettoyage de la base cela est <=> la ville ne contenait aucune lettre, espace ou tiret)
df_filtrage = df_filtrage[df_filtrage["city"].apply(lambda x: x.strip() != "")]

# Supprimer les city qui contiennent moins de 2 lettres distinctes
df_filtrage = df_filtrage[df_filtrage["city"].apply(lambda x: len(set(c.lower() for c in x if c.isalpha())))>=2]

## street
# Supprimer les street qui sont des adresses mails
df_filtrage = df_filtrage[~df_filtrage["street"].str.contains("@")]

# Supprimer les street qui sont des floats ou int
df_filtrage = df_filtrage[~df_filtrage["street"].str.strip().str.contains('^[0-9,.]+$')]

# Supprimer les lignes où la colonne 'street' contient des caractères spéciaux
df_filtrage = df_filtrage[~df_filtrage["street"].str.strip().str.contains("[^A-Za-z0-9\s,\'-.']")]


## street & address_1 & address_2
# Supprimer les street & address_1 & address_2 qui contiennent moins de 4 lettres distinctes
df_filtrage = df_filtrage[(df_filtrage["street"] + df_filtrage["address_1"] + df_filtrage["address_2"]).apply(lambda x: len(set(c.lower() for c in x if c.isalpha())))>3]

# Supprimer les street & address_1 & address_2 qui sont de tailles inférieurs à 5
df_filtrage = df_filtrage[(df_filtrage["street"] + df_filtrage["address_1"] + df_filtrage["address_2"]).str.strip().str.len() > 5]

print("Début du process de mise sous bon format.")
### Mise sous bon format

## city
# Mettre tout en majusucle sans accent et sans tiret
df_filtrage["city"] = df_filtrage["city"].apply(lambda x: unidecode(x).upper()).str.replace('-', ' ').str.strip()

# Remplacer les saint et saintes par les abréviations
df_filtrage["city"] = df_filtrage["city"].str.replace("SAINT ", "ST ")
df_filtrage["city"] = df_filtrage["city"].str.replace("SAINTE", "STE")

# Mettre tout en majusucle sans accent
df_filtrage["street"] = df_filtrage["street"].str.upper().str.strip()
df_filtrage["nom"] = df_filtrage["nom"].apply(lambda x: unidecode(x).upper()).str.strip()
df_filtrage["prenom"] = df_filtrage["prenom"].apply(lambda x: unidecode(x).upper()).str.strip()

## street
# Remplacer les "-" par des espaces
df_filtrage["street"] = df_filtrage["street"].str.replace("-", " ")
# Remplacer les "'" par des espaces
df_filtrage["street"] = df_filtrage["street"].str.replace("'", " ")

## address_1
# Mettre tout en majusucle sans accent
df_filtrage["address_1"] = df_filtrage["address_1"].apply(lambda x: unidecode(x).upper()).str.strip()
## address_2
df_filtrage["address_2"] = df_filtrage["address_2"].apply(lambda x: unidecode(x).upper()).str.strip()

### Nettoyage final

## street et address_1
# Si les deux colonnes sont égales, on supprime la valeur de address_1
df_filtrage.loc[df_filtrage["address_1"] == df_filtrage["street"], "address_1"] = ""
## street
# Supprimer les virgules et les remplacer par des espaces
df_filtrage['street'] = df_filtrage['street'].str.replace(",", " ").str.replace("  ", " ")
# Retirer les "A SAISIR"
df_filtrage['street'] = df_filtrage['street'].str.replace("A SAISIR", "").str.replace("ASAISIR", "").str.replace("SAISIR", "")
# Retirer les "A COMPLETER"
df_filtrage['street'] = df_filtrage['street'].str.replace("A COMPLETER", "").str.replace("ACOMPLETER", "").str.replace("COMPLETER", "")
# Supprimer les street vide
df_filtrage = df_filtrage[df_filtrage["street"]!=""]

# Retirer les doublons par rapport à la combinaison Nom, postal_code
doublons = df_filtrage.loc[df_filtrage.duplicated(["nom", "postal_code"]), "customer_cd"]
df_filtrage = df_filtrage.drop_duplicates(
    ["nom", "postal_code"]
)

#print(" Vérification de la cohérence du code postal et du nom de ville")
### VERIFIER LA COHERENCE DU CODE POSTAL ET DE LA VILLE
# Importation de la base des codes postaux

df_codes_postaux['Nom_de_la_commune'] = df_codes_postaux['Nom_de_la_commune'].str.replace(r'[^a-zA-Z -]+', '', regex=True).str.strip()

df_codes_postaux['Code_postal'] = df_codes_postaux['Code_postal'].apply(
    lambda x: '0' + str(x) if len(str(x))==4 else str(x)
)

df_final1 = pd.merge(
    df_filtrage,
    df_codes_postaux,
    left_on = ["city", "postal_code"],
    right_on=["Nom_de_la_commune", "Code_postal"],
    how = 'left'
)

df_final2 = pd.merge(
    df_final1,
    df_codes_postaux,
    left_on = ["city", "postal_code"],
    right_on=["Ligne_5", "Code_postal"],
    how = 'left'
)[list(df_clean.columns) + ["Nom_de_la_commune_x", "Nom_de_la_commune_y", "Ligne_5_y"] ]

df_conc = df_final2.query("Nom_de_la_commune_x.notna() or Nom_de_la_commune_y.notna()")

df_conc.loc[:, 'city'] = df_conc['Nom_de_la_commune_x'].fillna(df_conc['Nom_de_la_commune_y'])
df_conc.loc[:, "postal_locality"] = df_conc["Ligne_5_y"].fillna(df_conc["postal_locality"])

df_conc = df_conc[["customer_cd", "nom", "prenom", "email", "street", "address_1", "address_2", "postal_locality", "city", "postal_code", "country_cd", "language_cd", "score"]]

df_conc = df_conc.replace(['nan', 'NAN', np.nan], '')
df_conc["score"] = df_conc["score"].astype(int)

### NETTOYAGE DES DEMANDEURS
## postal_code
## Mettre les codes postaux en taille 5 uniquement pour les FR
df_demandeurs.loc[df_demandeurs["country_cd"].str.upper()=='FR', 'postal_code'] = df_demandeurs.loc[df_demandeurs["country_cd"].str.upper()=='FR','postal_code'].apply(
    lambda x: '0' + str(x) if len(str(x))==4 else str(x)
)

## street
## Retirer la ville dans street quand elle est présente
city_in_street = df_demandeurs.apply(
    lambda x: (str(x["city"]) in str(x["street"])),
    axis = 1
)
df_demandeurs[city_in_street].apply(lambda x: str(x["street"]).replace(str(x["city"]), ""), axis=1)

## Retirer le code postal dans street quand il est présent
postal_code_in_street = df_demandeurs.apply(
    lambda x: (str(x["postal_code"]) in str(x["street"])),
    axis = 1
)
df_demandeurs.loc[postal_code_in_street,"street"] = df_demandeurs[postal_code_in_street].apply(lambda x: str(x["street"]).replace(str(x["postal_code"]), ""), axis=1)

## Retirer ce qui n'est pas une lettre ou un chiffre au début
df_clean['street'] = df_clean['street'].replace(r'^[^a-zA-Z-]*', '', regex=True)

## Mettre les adresses correctement quand street est un chiffre
df_demandeurs.loc[(df_demandeurs['street'].astype(str).str.match(r'^\d+$')) | (df_demandeurs['street'].astype(str) ==""), "street"] = df_demandeurs[(df_demandeurs['street'].astype(str).str.match(r'^\d+$')) | (df_demandeurs['street'].astype(str) =="")].apply(
lambda x: (str(x["street"]) + str(x["address_1"])) if not pd.isna(x["address_1"]) else (str(x["street"]) + " " + str(x["address_2"])),
axis = 1 
)

## On met toutes les colonnes en majuscule et sans accent
df_demandeurs["nom"] = df_demandeurs["nom"].apply(lambda x: unidecode(x).upper()).str.strip()
df_demandeurs["prenom"] = df_demandeurs["prenom"].apply(lambda x: unidecode(x).upper()).str.strip()
df_demandeurs["language_cd"] = df_demandeurs["language_cd"].apply(lambda x: unidecode(x).upper()).str.strip()
df_demandeurs["street"] = df_demandeurs["street"].apply(lambda x: unidecode(x).upper()).str.strip()
df_demandeurs["city"] = df_demandeurs["city"].apply(lambda x: unidecode(x).upper()).str.strip()
df_demandeurs["address_1"] = df_demandeurs["address_1"].apply(lambda x: unidecode(x).upper() if isinstance(x, str) else x).str.strip()
df_demandeurs["address_2"] = df_demandeurs["address_2"].apply(lambda x: unidecode(x).upper() if isinstance(x, str) else x).str.strip()

## Ajout d'un score aux demandeurs qui doit être le plus élevé de toute la base catalogue
df_demandeurs["score"] = int("1" * (len(str(df_conc["score"].max()))+1))
df_demandeurs["postal_locality"] = np.nan

final_file_name = input("Nom du fichier final (sans extention) :")
lines_nb = int(input("Nombre de lignes à extraire :"))

df_concat = pd.concat([df_demandeurs, df_conc], ignore_index=True).sort_values(
    "score", ascending=False
    ).drop_duplicates('email')

### Nettoyage final
## On met les prenoms noms dans une seule colonne
df_concat["NOM PRENOM"] = df_concat["nom"] + " " + df_concat["prenom"]
df_catalogue_before_routeur = df_concat.drop(["prenom", "nom"], axis=1)

## On met le language en minuscule
df_catalogue_before_routeur["language_cd"] = df_catalogue_before_routeur["language_cd"].str.lower()

## On met les codes postaux et ville dans une seule colonne
df_catalogue_before_routeur["CODE POSTAL VILLE"] = df_catalogue_before_routeur["postal_code"].astype(str) + " " + df_catalogue_before_routeur["city"]
df_catalogue_before_routeur = df_catalogue_before_routeur.drop(["postal_code", "city"], axis=1)

df_catalogue_before_routeur = df_catalogue_before_routeur.rename(
    columns={
        "customer_cd": "MSF_NCLI",
        "NOM PRENOM": "AD1",
        "street": "AD4",
        "postal_locality": "AD5",
        "address_1": "AD2",
        "address_2": "AD3",
        "postal_code": "AD6",
        "country_cd": "PAYS",
        "language_cd": "ECLATE",
        "CODE POSTAL VILLE": "AD6"
    })

df_catalogue_before_routeur.drop(
    ["score", "email"], axis = 1
)[
    ['MSF_NCLI', 'AD1', 'AD4', 'AD5', 'AD2', 'AD3', 'AD6', 'PAYS', 'ECLATE']
  ].head(lines_nb).to_excel(
    path + final_file_name + ".xlsx",
    index=False
)

print("Export réussi. {} lignes ont été filtrées.".format(df_init.shape[0] - df_conc.shape[0]))
