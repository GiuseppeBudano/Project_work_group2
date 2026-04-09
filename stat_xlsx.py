import pandas as pd
import numpy as np

def safe_sheet_name(name, max_len=31):
    """
    Rende il nome del foglio compatibile con Excel.
    """
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
    for ch in invalid_chars:
        name = name.replace(ch, "_")
    return name[:max_len]


def export_statistiche_excel(datasets, nome_file="report_statistiche.xlsx"):
    """
    Esporta le statistiche dei dataset in un file Excel con più fogli.

Parameters
    ----------
    datasets : dict
        Dizionario nel formato {nome_dataset: dataframe}.
    nome_file : str, default="report_statistiche.xlsx"
        Nome del file Excel di output.
    """
    if not datasets:
        print("Nessun dataset disponibile da esportare.")
        return

    with pd.ExcelWriter(nome_file, engine="openpyxl") as writer:

        # Foglio riepilogativo iniziale
        riepilogo = pd.DataFrame([
            {
                "Dataset": nome,
                "Numero righe": df.shape[0],
                "Numero colonne": df.shape[1]
            }
            for nome, df in datasets.items()
        ])
        riepilogo.to_excel(writer, sheet_name="riepilogo", index=False)

        for nome, df in datasets.items():
            base_name = safe_sheet_name(nome)

            # 1. INFO GENERALI
            info = pd.DataFrame({
                "Metrica": ["Numero righe", "Numero colonne"],
                "Valore": [df.shape[0], df.shape[1]]
            })
            info.to_excel(writer, sheet_name=safe_sheet_name(f"{base_name}_info"), index=False)

            # 2. MISSING VALUES
            missing = df.isnull().sum()
            missing = missing[missing > 0]

            if not missing.empty:
                percentuali = (df.isnull().mean() * 100)[missing.index].round(2)

                missing_df = pd.DataFrame({
                    "Colonna": missing.index,
                    "Missing": missing.values,
                    "Percentuale (%)": percentuali.values
                })

                missing_df.to_excel(
                    writer,
                    sheet_name=safe_sheet_name(f"{base_name}_missing"),
                    index=False
                )

            # 3. STATISTICHE NUMERICHE
            numeriche = df.select_dtypes(include=np.number).copy()
            numeriche = numeriche.drop(
                columns=[
                    col for col in numeriche.columns
                    if "ID" in col or col in ["ORDER_YEAR", "ORDER_MONTH", "ORDER_WEEK"]
                ],
                errors="ignore"
            )

            if not numeriche.empty:
                stats = numeriche.describe().T.round(2)
                stats.reset_index(inplace=True)
                stats.rename(columns={"index": "Variabile"}, inplace=True)
                stats.to_excel(
                    writer,
                    sheet_name=safe_sheet_name(f"{base_name}_numeriche"),
                    index=False
                )

            # 4. TOP CATEGORICHE
            categoriche = df.select_dtypes(include=["object", "category", "bool"])

            risultati_cat = []

            for col in categoriche.columns:
                freq = df[col].value_counts(dropna=False).head(5)
                perc = (df[col].value_counts(normalize=True, dropna=False).head(5) * 100).round(2)

                tmp = pd.DataFrame({
                    "Variabile": col,
                    "Valore": freq.index.astype(str),
                    "Frequenza": freq.values,
                    "Percentuale (%)": perc.values
                })

                risultati_cat.append(tmp)

            if risultati_cat:
                cat_df = pd.concat(risultati_cat, ignore_index=True)
                cat_df.to_excel(
                    writer,
                    sheet_name=safe_sheet_name(f"{base_name}_categoriche"),
                    index=False
                )

    print(f"Report Excel salvato in: {nome_file}")