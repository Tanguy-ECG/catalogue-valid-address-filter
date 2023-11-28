import pandas as pd

df_HV = pd.read_csv(
    "HV/HV_catalogs_cleaned.csv",
    dtype = {"postal_code": str}
).assign(segment='Homair')

df_TH = pd.read_csv(
    "TOHAPI/TOHAPI_catalogs_cleaned.csv",
    dtype = {"postal_code": str}
).assign(segment='Tohapi')

### Retirer les Tohapi dans les Homair (doublons)
## adresse mail en double
adress_mail_Th_in_HV = df_TH["email"].isin(df_HV["email"])
df_TH = df_TH[(~adress_mail_Th_in_HV)|(df_TH["type"] == 'Demandeur')]
print("{} lignes supprimées pour doublon sur les emails".format(adress_mail_Th_in_HV.sum()))

## combinaison NOM + adresse + code postal
adress_Th_in_HV = df_TH.assign(nom_adress_postal_code=df_TH['nom'] + df_TH['street'] + df_TH['postal_code'].astype(str))["nom_adress_postal_code"].isin(
    df_HV.assign(nom_adress_postal_code=df_HV['nom'] + df_HV['street'] + df_HV['postal_code'].astype(str))["nom_adress_postal_code"]
)
df_TH = df_TH[(~adress_Th_in_HV)|(df_TH["type"] == 'Demandeur')]
print("{} lignes supprimées pour doublon sur le NOM + adresse + code postal".format(adress_Th_in_HV.sum()))

## Sélectionner 10000 lignes aléatoirement dans df_TH, les retirer de df_TH et les ajouter à df_HV.
# Sélectionner 10,000 lignes aléatoires dans df_TH
selected_rows = df_TH.query(
    "type != 'Demandeur'"
).sample(n=10000, random_state=42)

# Retirer les lignes sélectionnées de df_TH
df_TH = df_TH.drop(selected_rows.index)

# Ajouter les lignes sélectionnées à df_HV
df_HV = pd.concat([df_HV, selected_rows], ignore_index=True)

def sort_HV(row):
    if (row["segment"] == "Homair") & (row["type"] == "Demandeur"):
        return 0
    if (row["segment"] == "Tohapi"):
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
df_HV["NOM PRENOM"] = df_HV["nom"] + " " + df_HV["prenom"].str.replace(".", "").str.replace("*", "")
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
    ].query(
        "ECLATE == 'fr'"
    )

###
# TOHAPI
###
## On met les prenoms noms dans une seule colonne
df_TH["NOM PRENOM"] = (df_TH["nom"] + " " + df_TH["prenom"]).str.replace(".", "").str.replace("*", "")
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
    ].query(
        "ECLATE == 'fr'"
    )


### Associer les codes fidélités
## TOHAPI
df_fidelite_code_TH = pd.read_csv(
    "CODE FIDELITE/20231128-tohapi-codes-fidelite.csv",
    sep = ";",
).rename(
    columns={
        "MSF_NCLI": "Code client détenteur de l'avantage",
        "TH_fid": "Montant",
        "Tohapi_code": "Code"
    }
)

df_TH_catalogue_before_routeur = pd.merge(
    df_TH_catalogue_before_routeur,
    df_fidelite_code_TH,
    left_on="MSF_NCLI",
    right_on="Code client détenteur de l'avantage",
    how="left"
).fillna("")

## HOMAIR VACANCES
df_fidelite_code_HV = pd.read_csv(
    "CODE FIDELITE/20231128-fid_existant.csv",
    sep = ";",
    usecols=["Code client détenteur de l'avantage", "Code", "Montant", "Statut validité"]
).query("`Statut validité` == '8 - Valide'").dropna().drop_duplicates("Code client détenteur de l'avantage").drop(
    "Statut validité",
    axis=1
)

df_HV_catalogue_before_routeur = pd.merge(
    df_HV_catalogue_before_routeur,
    pd.concat([df_fidelite_code_HV, df_fidelite_code_TH], ignore_index=True),
    left_on="MSF_NCLI",
    right_on="Code client détenteur de l'avantage",
    how="left"
).drop(
    "Code client détenteur de l'avantage",
    axis=1
).fillna("")

## EXPORT
print("Nombre de lignes finales dans le HV : {}".format(df_HV_catalogue_before_routeur.shape[0]))
print("Debut de l'export en fichiers excels pour HV")
###
# HOMAIR
###
# list_montant_code = list(df_HV_catalogue_before_routeur["Montant"].str.replace(",00 EUR", "").unique())

# for montant_code in list_montant_code:
#     if montant_code=="":
#         montant_code_file = "0"
#     else:
#         montant_code_file = montant_code
#     df_HV_catalogue_before_routeur[df_HV_catalogue_before_routeur["Montant"].str.replace(",00 EUR", "")==montant_code].to_excel(
#         "HV/HV_{}_catalogs_for_router.xlsx".format(montant_code_file),
#         index = False
#     )

# print("Nombre de lignes finales dans le TOHAPI : {}".format(df_TH_catalogue_before_routeur.shape[0]))
# print("Debut de l'export en fichiers excels pour TOHAPI")
# ###
# # TOHAPI
# ###
# list_montant_code = list(df_TH_catalogue_before_routeur["Montant"].str.replace(",00 EUR", "").unique())

# for montant_code in list_montant_code:
#     if montant_code=="":
#         montant_code_file = "0"
#     else:
#         montant_code_file = montant_code
#     df_TH_catalogue_before_routeur[df_TH_catalogue_before_routeur["Montant"].str.replace(",00 EUR", "")==montant_code].to_excel(
#         "TOHAPI/TH_{}_catalogs_for_router.xlsx".format(montant_code_file),
#         index = False
#     )
# print("Fichiers correctement exportés.")
