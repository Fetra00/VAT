from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
import pandas as pd
import numpy as np
from io import BytesIO
import pandas as pd
import numpy as np

app = FastAPI()

@app.post("/process_excel")
async def process_excel(file: UploadFile = File(...)):
    data = pd.read_excel(file.file)
    data_1 = data[data['codeCompte'].astype(str).str.startswith('512')]
    mois_filtre = 5 
    annee_filtre = 2025 
    data_1['pDate'] = pd.to_datetime(data_1['pDate'], dayfirst=True, errors='coerce')
    data_1 = data_1[
        (data_1['pDate'].dt.month == mois_filtre) &
        (data_1['pDate'].dt.year == annee_filtre)
    ]

    banque = data_1.copy()
    data_2 = data[data['codeCompte'].astype(str).str.startswith('53')]
    data_2['pDate'] = pd.to_datetime(data_2['pDate'], dayfirst=True, errors='coerce')
    data_2 = data_2[
        (data_2['pDate'].dt.month == mois_filtre) &
        (data_2['pDate'].dt.year == annee_filtre)
    ]
    mvola = data_2.copy()

    liste_contrepartie_banque = set(banque['NumeroPiece'].tolist())
    liste_contrepartie_mvola = set(mvola['NumeroPiece'].tolist())
    contrepartie_banque = data[data['NumeroPiece'].isin(liste_contrepartie_banque)]
    contrepartie_mvola = data[data['NumeroPiece'].isin(liste_contrepartie_mvola)]
    contrepartie_banque = contrepartie_banque[~contrepartie_banque['codeCompte'].astype(str).str.startswith('512')]
    contrepartie_mvola = contrepartie_mvola[~contrepartie_mvola['codeCompte'].astype(str).str.startswith('53')]

    lettre_banque = contrepartie_banque[contrepartie_banque['Lettre'].notna() & (contrepartie_banque['Lettre'].astype(str).str.strip().ne(""))] 
    lettre_mvola = contrepartie_mvola[contrepartie_mvola['Lettre'].notna() & contrepartie_mvola['Lettre'].astype(str).str.strip().ne("")]

    liste_lettre_banque = lettre_banque['Lettre'].tolist()
    liste_lettre_mvola = lettre_mvola['Lettre'].tolist()
    achat_vente_banque = data[data['Lettre'].isin(liste_lettre_banque)]
    achat_vente_mvola = data[data['Lettre'].isin(liste_lettre_mvola)]
    achat_vente_banque = achat_vente_banque[
        ~achat_vente_banque['NumeroPiece'].astype(str).str.contains("BNI", case=False, na=False)
    ]
    achat_vente_mvola = achat_vente_mvola[
        ~achat_vente_mvola['NumeroPiece'].astype(str).str.contains("BNI", case=False, na=False)
    ]

    fichier_lettre_banque = lettre_banque[[
        'Lettre',
        'NumeroPiece',
        'MontantMGA',
        'pDate']]
    fichier_lettre_mvola = lettre_mvola[[
        'Lettre',
        'NumeroPiece',
        'MontantMGA',
        'pDate']]
    fichier_achat_vte_banque = achat_vente_banque[[
        'Lettre',
        'NumeroPiece',
        'RAISON SOCIAL',
        'Caption',
        'pDate']]
    fichier_achat_vte_mvola = achat_vente_mvola[[
        'Lettre',
        'NumeroPiece',
        'RAISON SOCIAL',
        'Caption',
        'pDate']]
    fichier_lettre_banque_renamed = fichier_lettre_banque.rename(columns={
        "pDate": "Date de paiement",
        "MontantMGA": "Montant payé",
        "NumeroPiece" : "Banque"})
    fichier_lettre_mvola_renamed = fichier_lettre_mvola.rename(columns={
        "pDate": "Date de paiement",
        "MontantMGA": "Montant payé",
        "NumeroPiece" : "Banque"})
    fichier_achat_vte_banque = fichier_achat_vte_banque.merge(
        fichier_lettre_banque_renamed[[
            "Lettre", 
            "Date de paiement", 
            "Montant payé",
            "Banque"]],
        on="Lettre",
        how="left")
    fichier_achat_vte_mvola = fichier_achat_vte_mvola.merge(
        fichier_lettre_mvola_renamed[[
            "Lettre", 
            "Date de paiement", 
            "Montant payé",
            "Banque"]],
        on="Lettre",
        how="left")

    liste_achat_vente_banque = set(achat_vente_banque['NumeroPiece'].tolist())
    liste_achat_vente_mvola = set(achat_vente_mvola['NumeroPiece'].tolist())
    TVA_banque = data[data['NumeroPiece'].isin(liste_achat_vente_banque)]
    TVA_mvola = data[data['NumeroPiece'].isin(liste_achat_vente_mvola)]
    TVA = pd.concat([TVA_banque, TVA_mvola], ignore_index=True)
    TVA["codeCompte"] = TVA["codeCompte"].astype(str)
    TVA["TYPE"] = np.select(
        [
            TVA["codeCompte"].str.startswith(("6", "7")),       
            TVA["codeCompte"].isin(["4456300", "4457200"]),    
            TVA["codeCompte"].isin(["4456200", "4456100"]),    
            TVA["codeCompte"].str.startswith(("401", "411")), 
            TVA["codeCompte"].str.startswith("486"),           
            TVA["codeCompte"].eq("") | TVA["codeCompte"].isna(),
            TVA["codeCompte"].str.startswith("2"),             
            TVA["codeCompte"].str.startswith("409"),         
        ],
        [
            "MONTANT HT",
            "TVA SUR SERVICE",
            "TVA SUR BIEN",
            "MONTANT TTC",
            "CCA",
            "ERREUR",
            "IMMOBILISATION",
            "AVANCE",
        ],
        default="AUTRES"
    )
    TCD = TVA.pivot_table(
        index=["NumeroPiece", "referenceOrigine"],
        columns="TYPE",
        values="MontantMGA",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    TCD["TOTAL GENERAL"] = TCD.iloc[:, 2:].sum(axis=1)
    total_general = TCD.iloc[:, 2:].sum().to_frame().T
    total_general["NumeroPiece"] = "TOTAL GENERAL"
    total_general["referenceOrigine"] = ""
    total_general = total_general[TCD.columns]
    TCD = pd.concat([TCD, total_general], ignore_index=True)

    lettre_banque_vide = contrepartie_banque[
        contrepartie_banque['Lettre'].isna() | (contrepartie_banque['Lettre'].astype(str).str.strip() == "")
    ]
    lettre_mvola_vide = contrepartie_mvola[
        contrepartie_mvola['Lettre'].isna() | (contrepartie_mvola['Lettre'].astype(str).str.strip() == "")
    ]
    lettre_vide = pd.concat(
        [lettre_banque_vide, lettre_mvola_vide],
        ignore_index=True
    )
    lettre_vide["codeCompte"] = lettre_vide["codeCompte"].astype(str)
    lettre_vide["TYPE"] = np.select(
        [
            lettre_vide["codeCompte"].str.startswith(("6", "7")),      
            lettre_vide["codeCompte"].isin(["4456300", "4457200"]),   
            lettre_vide["codeCompte"].isin(["4456200", "4456100"]),   
            lettre_vide["codeCompte"].str.startswith(("401", "411")), 
            lettre_vide["codeCompte"].str.startswith("486"),          
            lettre_vide["codeCompte"].eq("") | lettre_vide["codeCompte"].isna(),
            lettre_vide["codeCompte"].str.startswith("2"),            
            lettre_vide["codeCompte"].str.startswith("409"),          
        ],
        [
            "MONTANT HT",
            "TVA SUR SERVICE",
            "TVA SUR BIEN",
            "MONTANT TTC",
            "CCA",
            "ERREUR",
            "IMMOBILISATION",
            "AVANCE",
        ],
        default="AUTRES"
    )
    non_lettre = lettre_vide.pivot_table(
        index=["NumeroPiece", "referenceOrigine"],
        columns="TYPE",
        values="MontantMGA",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    non_lettre["TOTAL GENERAL"] = non_lettre.iloc[:, 2:].sum(axis=1)
    total_general = non_lettre.iloc[:, 2:].sum().to_frame().T
    total_general["NumeroPiece"] = "TOTAL GENERAL"
    total_general["referenceOrigine"] = ""
    total_general = total_general[non_lettre.columns]
    non_lettre = pd.concat([non_lettre, total_general], ignore_index=True)

    LIEN = pd.concat(
        [fichier_achat_vte_banque, fichier_achat_vte_mvola],
        ignore_index=True
    )
    fichier_de_suivi = TCD.merge(
        LIEN.drop_duplicates("NumeroPiece")[["Date de paiement", "Banque", "Caption","NumeroPiece","RAISON SOCIAL"]],
        on="NumeroPiece",
        how="left"
    )
    fichier_de_suivi["FOURNISSEUR(F) ou CLIENT(C)"] = fichier_de_suivi["MONTANT TTC"].apply(lambda x : "C" if x >= 0 else "F")

    fichier_de_suivi["Nature"] = np.where(
        (fichier_de_suivi["TVA SUR BIEN"] == 0) &
        (fichier_de_suivi["TVA SUR SERVICE"] == 0),
        "NEUTRE",
        np.where(
            (fichier_de_suivi["IMMOBILISATION"] == 0) &
            (fichier_de_suivi["TVA SUR SERVICE"].notna()) &
            (fichier_de_suivi["TVA SUR SERVICE"] != 0),
            "S",
            np.where(
                (fichier_de_suivi["IMMOBILISATION"] == 0) &
                (fichier_de_suivi["TVA SUR BIEN"].notna()) &
                (fichier_de_suivi["TVA SUR BIEN"] != 0),
                "B",
                "I"
            )
        )
    )
    fichier_de_suivi["TVA"] = np.where(
        fichier_de_suivi["TVA SUR BIEN"] == 0 | (fichier_de_suivi["TVA SUR BIEN"] == ""),
        fichier_de_suivi["TVA SUR SERVICE"],
        fichier_de_suivi["TVA SUR BIEN"]
    )

    dfs = {
        "banque": banque,
        "mvola": mvola,
        "contrepartie_banque": contrepartie_banque,
        "contrepartie_mvola": contrepartie_mvola,
        "lettre_banque": lettre_banque,
        "lettre_mvola": lettre_mvola,
        "achat_vente_banque": achat_vente_banque,
        "achat_vente_mvola": achat_vente_mvola,
        "fichier_achat_vte_banque": fichier_achat_vte_banque,
        "fichier_achat_vte_mvola": fichier_achat_vte_mvola,
        "TVA": TVA,
        "TCD": TCD,
        "lettre_banque_vide": lettre_banque_vide,
        "lettre_mvola_vide": lettre_mvola_vide,
        "lettre_vide": lettre_vide,
        "non_lettre": non_lettre,
        "fichier_de_suivi": fichier_de_suivi
    }

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet_name, df in dfs.items():
            sheet_name_safe = sheet_name[:31]
            df.to_excel(writer, sheet_name=sheet_name_safe, index=False)
            worksheet = writer.sheets[sheet_name_safe]
            workbook = writer.book
            header_format = workbook.add_format({'bold': True})
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            for i, col in enumerate(df.columns):
                max_len = df[col].astype(str).map(len).max()
                max_len = max(max_len, len(col)) + 2  
                worksheet.set_column(i, i, max_len)
    
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=resultat.xlsx"}
    )
