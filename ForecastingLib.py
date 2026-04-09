"""
Sales Pipeline – Funzioni modulari
===================================
Riceve un DataFrame OLAP prodotto da un altro membro del team
e applica: aggregazioni, training con regressione lineare, forecast, salvataggio CSV.

Utilizzo:
    import pandas as pd
    from sales_pipeline import aggrega_dati, addestra_modello, genera_previsioni, salva_csv

    olap = pd.read_csv("sales_olap.csv")

    aggregazioni      = aggrega_dati(olap)
    modello, metriche = addestra_modello(aggregazioni["mensile"])
    previsioni        = genera_previsioni(modello, aggregazioni["mensile"])
    salva_csv(aggregazioni, previsioni)
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Cartella dove vengono salvati i CSV di output
OUTPUT_DIR = "./output"


# =============================================================================
# 1. aggrega_dati
# =============================================================================

def aggrega_dati(olap):
    """
    Parte dal DataFrame OLAP (una riga per ordine) e produce tre tabelle
    aggregate pronte per Power BI e per il modello.

    Restituisce un dizionario con tre chiavi:
        "mensile"  -> totali per anno/mese/azienda/paese/business line
        "clienti"  -> totali per cliente, ordinati per fatturato decrescente
        "articoli" -> totali per articolo, ordinati per fatturato decrescente
    """

    # Raggruppa per mese + dimensioni e somma i valori economici
    mensile = olap.groupby(
        ["ORDER_YEAR", "ORDER_MONTH", "ID_COMPANY", "DESC_BUSINESS_LINE", "ID_COUNTRY"]
    ).agg(
        TOTAL_REVENUES = ("VAL_REVENUES", "sum"),
        TOTAL_COST     = ("VAL_COST",     "sum"),
        TOTAL_MARGIN   = ("VAL_MARGIN",   "sum"),
        N_ORDERS       = ("ID_ORDER_NUM", "count"),
    ).reset_index()

    mensile = mensile.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Raggruppa per cliente e somma
    clienti = olap.groupby(
        ["IDS_CUSTOMER", "DESC_CUSTOMER", "ID_COUNTRY", "DESC_AREA_MANAGER"]
    ).agg(
        TOTAL_REVENUES = ("VAL_REVENUES", "sum"),
        TOTAL_COST     = ("VAL_COST",     "sum"),
        TOTAL_MARGIN   = ("VAL_MARGIN",   "sum"),
        N_ORDERS       = ("ID_ORDER_NUM", "count"),
    ).reset_index()

    clienti = clienti.sort_values("TOTAL_REVENUES", ascending=False).reset_index(drop=True)

    # Raggruppa per articolo e somma
    articoli = olap.groupby(
        ["IDS_ITEM", "DESC_ITEM", "DESC_BUSINESS_LINE"]
    ).agg(
        TOTAL_REVENUES = ("VAL_REVENUES", "sum"),
        TOTAL_COST     = ("VAL_COST",     "sum"),
        TOTAL_MARGIN   = ("VAL_MARGIN",   "sum"),
        N_ORDERS       = ("ID_ORDER_NUM", "count"),
    ).reset_index()

    articoli = articoli.sort_values("TOTAL_REVENUES", ascending=False).reset_index(drop=True)

    print(f"Aggregazione mensile:  {len(mensile)} righe")
    print(f"Aggregazione clienti:  {len(clienti)} righe")
    print(f"Aggregazione articoli: {len(articoli)} righe")

    return {"mensile": mensile, "clienti": clienti, "articoli": articoli}


# =============================================================================
# 2. addestra_modello
# =============================================================================

def addestra_modello(mensile):
    """
    Riceve la tabella mensile, prepara le feature direttamente qui dentro
    e allena un modello di regressione lineare (LinearRegression di scikit-learn).

    La valutazione usa TimeSeriesSplit con 3 fold:
        - ogni fold aggiunge mesi al training e testa sui mesi successivi
        - le metriche riportate sono la media dei 3 fold (stima onesta su dati non visti)
    Dopo la valutazione, il modello viene riallenato su TUTTI i dati disponibili
    per avere la migliore stima possibile prima di fare le previsioni future.

    Le feature costruite sono:
        - MONTH_IDX  : indice progressivo del mese (0, 1, 2, ...) → cattura il trend
        - ORDER_MONTH: numero del mese (1-12) → cattura la stagionalità
        - LAG_1      : ricavo del mese precedente → il passato recente predice il futuro

    Restituisce:
        modello   -> LinearRegression allenato su tutti i dati, pronto per predict()
        metriche  -> dizionario con MAE e R² medi sui fold di cross-validation
    """

    # --- Consolida per mese (somma su azienda, paese, business line) ----------
    df = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        TOTAL_REVENUES = ("TOTAL_REVENUES", "sum"),
        N_ORDERS       = ("N_ORDERS",       "sum"),
    ).reset_index()

    df = df.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Indice progressivo del mese: 0 per il primo mese, 1 per il secondo, ecc.
    df["MONTH_IDX"] = range(len(df))

    # Ricavo del mese precedente (shift(1) sposta i valori di una riga in giù)
    df["LAG_1"] = df["TOTAL_REVENUES"].shift(1)

    # --- Rimuovi la prima riga che ha NaN in LAG_1 ----------------------------
    df = df.dropna(subset=["LAG_1"]).reset_index(drop=True)

    # --- Prepara X (input) e y (output) per il modello ------------------------
    # X contiene tre colonne: indice del mese, numero del mese, ricavo mese scorso
    X = df[["MONTH_IDX", "ORDER_MONTH", "LAG_1"]].values

    # y contiene i ricavi reali che il modello deve imparare a prevedere
    y = df["TOTAL_REVENUES"].values

    print(f"Campioni totali disponibili: {len(X)}")
    print(f"Feature usate: MONTH_IDX, ORDER_MONTH, LAG_1")

    # --- Valutazione con TimeSeriesSplit (3 fold) ------------------------------
    # TimeSeriesSplit divide i dati in modo cronologico:
    #   fold 1 → train sui primi mesi,  test sui mesi successivi
    #   fold 2 → train su più mesi,     test sui mesi ancora successivi
    #   fold 3 → train su ancora più mesi, test sugli ultimi mesi
    # In questo modo il modello viene sempre valutato su dati che non ha mai visto
    # e che sono cronologicamente successivi al training (come nella realtà).

    tscv = TimeSeriesSplit(n_splits=3)

    mae_per_fold  = []
    rmse_per_fold = []
    r2_per_fold   = []

    print("\n  Risultati per fold:")
    print(f"  {'Fold':<6} {'Train':>8} {'Test':>6} {'MAE':>12} {'RMSE':>12} {'R²':>8}")
    print("  " + "-" * 56)

    for numero_fold, (indici_train, indici_test) in enumerate(tscv.split(X), start=1):

        # Estrae i campioni di training e di test per questo fold
        X_train = X[indici_train]
        y_train = y[indici_train]
        X_test  = X[indici_test]
        y_test  = y[indici_test]

        # Allena il modello solo sui dati di training di questo fold
        modello_fold = LinearRegression()
        modello_fold.fit(X_train, y_train)

        # Valuta il modello sui dati di test (mai visti durante il training)
        y_test_previsto = modello_fold.predict(X_test)

        mae_fold  = mean_absolute_error(y_test, y_test_previsto)
        rmse_fold = np.sqrt(mean_squared_error(y_test, y_test_previsto))
        r2_fold   = r2_score(y_test, y_test_previsto)

        mae_per_fold.append(mae_fold)
        rmse_per_fold.append(rmse_fold)
        r2_per_fold.append(r2_fold)

        print(f"  {numero_fold:<6} {len(X_train):>8} {len(X_test):>6} {mae_fold:>12,.0f} {rmse_fold:>12,.0f} {r2_fold:>8.4f}")

    # Medie sui fold: questa è la stima più onesta delle prestazioni reali del modello
    mae_medio  = np.mean(mae_per_fold)
    rmse_medio = np.mean(rmse_per_fold)
    r2_medio   = np.mean(r2_per_fold)

    print(f"\n  Media sui 3 fold:")
    print(f"  MAE medio:  {mae_medio:,.0f}")
    print(f"  RMSE medio: {rmse_medio:,.0f}")
    print(f"  R² medio:   {r2_medio:.4f}")

    # --- Training finale su TUTTI i dati --------------------------------------
    # Dopo aver valutato il modello con la cross-validation,
    # lo rialleniamo su tutti i dati disponibili per avere
    # la migliore stima possibile da usare per le previsioni future.
    print(f"\n  Training finale su tutti i {len(X)} campioni...")
    modello = LinearRegression()
    modello.fit(X, y)

    metriche = {
        "MAE_medio_cv":  mae_medio,
        "RMSE_medio_cv": rmse_medio,
        "R2_medio_cv":   r2_medio,
        "mae_per_fold":  mae_per_fold,
        "r2_per_fold":   r2_per_fold,
    }

    return modello, metriche


# =============================================================================
# 3. genera_previsioni
# =============================================================================

def genera_previsioni(modello, mensile, n_mesi=6):
    """
    Genera le previsioni per i prossimi N mesi usando il modello di regressione lineare.

    Logica: per ogni mese futuro costruisce le tre feature (MONTH_IDX, ORDER_MONTH, LAG_1)
    e chiede al modello il ricavo previsto. Il valore previsto diventa subito il LAG_1
    del mese successivo (strategia auto-regressiva).

    Restituisce un DataFrame con una riga per ogni mese previsto.
    """

    # Consolida per mese per avere la serie storica pulita
    df = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        TOTAL_REVENUES = ("TOTAL_REVENUES", "sum"),
    ).reset_index()

    df = df.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Punto di partenza: ultimo mese disponibile nello storico
    ultimo_idx  = len(df) - 1
    ultimo_anno = int(df.iloc[-1]["ORDER_YEAR"])
    ultimo_mese = int(df.iloc[-1]["ORDER_MONTH"])

    # Il primo LAG_1 da usare è il ricavo dell'ultimo mese storico
    ultimo_ricavo = float(df.iloc[-1]["TOTAL_REVENUES"])

    righe_future = []

    for passo in range(1, n_mesi + 1):

        # Calcola il mese e l'anno del passo corrente
        mese_new = (ultimo_mese + passo - 1) % 12 + 1
        anno_new = ultimo_anno + ((ultimo_mese + passo - 1) // 12)
        idx_new  = ultimo_idx + passo

        # Costruisce il vettore delle tre feature per questo mese
        X_nuovo = np.array([[idx_new, mese_new, ultimo_ricavo]])

        # Chiede al modello la previsione e assicura che non sia negativa
        ricavo_previsto = float(modello.predict(X_nuovo)[0])
        ricavo_previsto = max(ricavo_previsto, 0)

        # Il valore previsto ora diventa il LAG_1 per il passo successivo
        ultimo_ricavo = ricavo_previsto

        righe_future.append({
            "ORDER_YEAR":        anno_new,
            "ORDER_MONTH":       mese_new,
            "DATE":              pd.Timestamp(anno_new, mese_new, 1),
            "FORECAST_REVENUES": round(ricavo_previsto, 2),
            "TIPO":              "PREVISIONE",
        })

    previsioni = pd.DataFrame(righe_future)
    print(previsioni[["DATE", "FORECAST_REVENUES"]].to_string(index=False))

    return previsioni


