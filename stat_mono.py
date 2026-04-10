import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def analisi_qualitativa(df, nome):
    """
    Esegue un'analisi qualitativa sintetica del DataFrame.
    """
    print("\n" + "=" * 80)
    print(f"DATASET: {nome.upper()}")
    print("=" * 80)

    righe, colonne = df.shape
    print(f"Dimensioni: {righe} righe x {colonne} colonne")

    missing = df.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)

    if not missing.empty:
        percentuali = (df.isnull().mean() * 100)[missing.index].round(2)
        missing_df = pd.DataFrame({
            "Missing": missing,
            "Percentuale (%)": percentuali
        })
        print("\nValori mancanti:")
        print(missing_df)
    else:
        print("\nValori mancanti: nessuno")

    unici = df.nunique().sort_values()
    print("\nNumero di valori unici per colonna:")
    print(unici)

    variabili_temporali = ["ORDER_YEAR","ORDER_MONTH","ORDER_WEEK"]

    numeriche = df.select_dtypes(include=np.number)
    numeriche = numeriche.drop(
        columns=[col for col in numeriche.columns if "ID" in col or col in variabili_temporali],
        errors="ignore"
    )

    if numeriche.shape[1] > 0:
        range_df = pd.DataFrame({
            "Min": numeriche.min(),
            "Max": numeriche.max()
        })
        print("\nRange variabili numeriche:")
        print(range_df.map(lambda x: f"{x:,.2f}"))
    else:
        print("\nNessuna variabile numerica rilevante")

#for nome, df in datasets.items():
    #analisi_qualitativa(df, nome)


# In[28]:


def statistiche_numeriche(df, nome):
    numeriche = df.select_dtypes(include=np.number)

    numeriche = numeriche.drop(columns=[col for col in numeriche.columns if "ID" in col or col in ["ORDER_YEAR", "ORDER_MONTH","ORDER_WEEK"]],errors="ignore")

    if numeriche.shape[1] == 0:
        print(f"\n{nome}: nessuna variabile numerica rilevante.")
        return

    print(f"\n{'='*70}")
    print(f"STATISTICHE NUMERICHE - {nome.upper()}")
    print(f"{'='*70}")

    stats = numeriche.describe().T.round(2)
    print(stats)



#for nome, df in datasets.items():
    #statistiche_numeriche(df, nome)


# Le statistiche descrittive mostrano che il valore medio dei ricavi (VAL_REVENUES) è significativamente superiore al valore medio dei costi (VAL_COST). Tale differenza indica la presenza di un margine operativo positivo, suggerendo che, in media, le operazioni di vendita generano valore economico. Inoltre, il rapporto tra ricavi e costi evidenzia una struttura favorevole in termini di redditività.

# In[29]:


def statistiche_categoriche(df, nome, top_n=10):
    """
    Calcola e visualizza le distribuzioni delle variabili categoriche.
    """
    categoriche = df.select_dtypes(include=["object", "category", "bool"])

    if categoriche.shape[1] == 0:
        print(f"\n{nome}: nessuna variabile categorica.")
        return

    print("\n" + "=" * 80)
    print(f"STATISTICHE CATEGORICHE - {nome.upper()}")
    print("=" * 80)

    for col in categoriche.columns:
        print(f"\nColonna: {col}")
        print("-" * 50)

        frequenze = df[col].value_counts(dropna=False).head(top_n)
        percentuali = (df[col].value_counts(normalize=True, dropna=False).head(top_n) * 100).round(2)

        risultato = pd.DataFrame({
            "Frequenza": frequenze,
            "Percentuale (%)": percentuali
        })

        print(risultato)



    print("\nNumero di valori unici per variabile categorica:")
    print(categoriche.nunique().sort_values(ascending=False))
