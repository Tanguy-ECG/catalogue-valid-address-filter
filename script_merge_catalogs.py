import pandas as pd

df_HV = pd.read_csv(
    "HV/HV_catalogs_cleaned.csv"
).assign(segment='Homair')

df_TH = pd.read_csv(
    "TOHAPI/TOHAPI_catalogs_cleaned.csv"
).assign(segment='Tohapi')


## Retirer les Tohapi dans les Homair
adress_Th_in_HV = df_TH.assign(nom_postal_code=df_TH['nom'] + df_TH['postal_code'].astype(str))["nom_postal_code"].isin(
    df_HV.assign(nom_postal_code=df_HV['nom'] + df_HV['postal_code'].astype(str))["nom_postal_code"]
)
df_TH = df_TH[~((adress_Th_in_HV) & (df_TH["type"] == "Non Demandeur"))]

## Sélectionner 10000 lignes aléatoirement dans df_TH, les retirer de df_TH et les ajouter à df_HV.
# Sélectionner 10,000 lignes aléatoires dans df_TH
selected_rows = df_TH.sample(n=10000, random_state=42)

# Retirer les lignes sélectionnées de df_TH
df_TH = df_TH.drop(selected_rows.index)

# Ajouter les lignes sélectionnées à df_HV
df_HV = pd.concat([df_HV, selected_rows], ignore_index=True)

def sort_HV(row):
    if (row["segment"] == "Homair") & (row["type"] == "Demandeur"):
        return 0
    if (row["segment"] == "TOHAPI"):
        return 1
    if (row["segment"] == "Homair") & (row["type"] == "Non Demandeur"):
        return 2
    return 3

df_HV["sort"] = df_HV.apply(
    sort_HV,
    axis = 1
)

df_HV = df_HV.sort_values(["sort", "score"], ascending=[True, False])


### Nettoyage final

###
# HOMAIR
###
## On met les prenoms noms dans une seule colonne
df_HV["NOM PRENOM"] = df_HV["nom"] + " " + df_HV["prenom"]
df_HV = df_HV.drop(["prenom", "nom"], axis=1)

## On met le language en minuscule
df_HV["language_cd"] = df_HV["language_cd"].str.lower()

## On met les codes postaux et ville dans une seule colonne
df_HV["CODE POSTAL VILLE"] = df_HV["postal_code"].astype(str) + " " + df_HV["city"]
df_HV = df_HV.drop(["postal_code", "city"], axis=1)

## On met le pays en 2 lettres
df_HV["country_cd"] = df_HV["country_cd"].str.slice(0, 2).str.upper()

df_HV_catalogue_before_routeur = df_HV.rename(
    columns={
        "customer_cd": "MSF_NCLI",
        "NOM PRENOM": "AD1",
        "street": "AD4",
        "postal_locality": "AD5",
        "address_1": "AD2",
        "address_2": "AD3",
        "country_cd": "PAYS",
        "language_cd": "ECLATE",
        "CODE POSTAL VILLE": "AD6"
    })
df_HV_catalogue_before_routeur = df_HV_catalogue_before_routeur[
        ['MSF_NCLI', 'AD1', 'AD4', 'AD5', 'AD2', 'AD3', 'AD6', 'PAYS', 'ECLATE']
    ]
df_HV_catalogue_before_routeur = df_HV_catalogue_before_routeur[
        ['MSF_NCLI', 'AD1', 'AD4', 'AD5', 'AD2', 'AD3', 'AD6', 'PAYS', 'ECLATE']
    ]

###
# TOHAPI
###
## On met les prenoms noms dans une seule colonne
df_TH["NOM PRENOM"] = df_TH["nom"] + " " + df_TH["prenom"]
df_TH = df_TH.drop(["prenom", "nom"], axis=1)

## On met le language en minuscule
df_TH["language_cd"] = df_TH["language_cd"].str.lower()

## On met les codes postaux et ville dans une seule colonne
df_TH["CODE POSTAL VILLE"] = df_TH["postal_code"].astype(str) + " " + df_TH["city"]
df_TH = df_TH.drop(["postal_code", "city"], axis=1)

## On met le pays en 2 lettres
df_TH["country_cd"] = df_TH["country_cd"].str.slice(0, 2).str.upper()

df_TH_catalogue_before_routeur = df_TH.rename(
    columns={
        "customer_cd": "MSF_NCLI",
        "NOM PRENOM": "AD1",
        "street": "AD4",
        "postal_locality": "AD5",
        "address_1": "AD2",
        "address_2": "AD3",
        "country_cd": "PAYS",
        "language_cd": "ECLATE",
        "CODE POSTAL VILLE": "AD6"
    })
df_TH_catalogue_before_routeur = df_TH_catalogue_before_routeur[
        ['MSF_NCLI', 'AD1', 'AD4', 'AD5', 'AD2', 'AD3', 'AD6', 'PAYS', 'ECLATE']
    ]
df_TH_catalogue_before_routeur = df_TH_catalogue_before_routeur[
        ['MSF_NCLI', 'AD1', 'AD4', 'AD5', 'AD2', 'AD3', 'AD6', 'PAYS', 'ECLATE']
    ]

## EXPORT
###
# HOMAIR
###
df_HV_catalogue_before_routeur.to_csv(
    "HV/HV_catalogs_for_router.csv",
    index = False
)

###
# TOHAPI
###
df_TH_catalogue_before_routeur.to_csv(
    "TOHAPI/TOHAPI_catalogs_for_router.csv",
    index = False
)
print("Fichiers correctement exportés.")
