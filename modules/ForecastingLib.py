"""
Sales Pipeline & Grafici Forecast — Libreria modulare
=======================================================
Libreria per l'analisi delle vendite storiche e la generazione di previsioni.
Sviluppata come parte del Project Work di analisi vendite.

Funzioni disponibili
--------------------
Aggregazione:
    aggrega_dati(olap)
        Produce tre tabelle aggregate (mensile, clienti, articoli)
        a partire dal DataFrame OLAP denormalizzato.

Modelli predittivi:
    addestra_modello(mensile)
        Regressione lineare con TimeSeriesSplit (3 fold).

    addestra_modello_gbr(mensile)
        GradientBoostingRegressor con TimeSeriesSplit (3 fold).

Forecast:
    genera_previsioni(modello, mensile, n_mesi=6)
        Genera previsioni mensili con strategia auto-regressiva.

Grafici:
    grafico_forecast_singolo(mensile, previsioni, nome_modello)
        Storico + una sola serie di previsioni.

    grafico_confronto_modelli(mensile, prev_lr, prev_gbr)
        Storico + Regressione lineare vs GradientBoosting.

    grafico_confronto_orizzonti(mensile, previsioni_dict)
        Storico + più orizzonti temporali a confronto.

"""

import os
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit


# Cartella di output di default per CSV e grafici
OUTPUT_DIR = "./output"


# =============================================================================
# 1. aggrega_dati
# =============================================================================

def aggrega_dati(olap):
    """
    Parte dal DataFrame OLAP (una riga per ordine) e produce tre tabelle
    aggregate pronte per Power BI e per il modello.

    Parametri
    ----------
    olap : DataFrame OLAP denormalizzato con colonne VAL_REVENUES, VAL_COST,
           VAL_MARGIN, ID_ORDER_NUM, ORDER_YEAR, ORDER_MONTH, ID_COMPANY,
           DESC_BUSINESS_LINE, ID_COUNTRY, IDS_CUSTOMER, DESC_CUSTOMER,
           DESC_AREA_MANAGER, IDS_ITEM, DESC_ITEM.

    Restituisce
    -----------
    Dizionario con tre chiavi:
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
    Allena un modello di regressione lineare sui dati mensili aggregati.

    La valutazione usa TimeSeriesSplit con 3 fold: il training è sempre
    nel passato e il test nei mesi successivi, rispettando l'ordine
    cronologico. Dopo la valutazione il modello viene riallenato su tutti
    i dati disponibili per produrre le previsioni finali.

    Feature costruite internamente:
        MONTH_IDX  : indice progressivo del mese (0, 1, 2, ...) → trend
        ORDER_MONTH: numero del mese (1-12) → stagionalità
        LAG_1      : ricavo del mese precedente → memoria a breve termine

    Parametri
    ----------
    mensile : DataFrame mensile (da aggrega_dati["mensile"])

    Restituisce
    -----------
    modello   : LinearRegression allenato su tutti i dati, pronto per predict()
    metriche  : dizionario con MAE, RMSE e R² medi sui fold di cross-validation
    """

    # Consolida per mese (somma su azienda, paese, business line)
    df = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        TOTAL_REVENUES = ("TOTAL_REVENUES", "sum"),
        N_ORDERS       = ("N_ORDERS",       "sum"),
    ).reset_index()

    df = df.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Indice progressivo del mese: 0 per il primo, 1 per il secondo, ecc.
    df["MONTH_IDX"] = range(len(df))

    # Ricavo del mese precedente (shift sposta i valori di una riga in giù)
    df["LAG_1"] = df["TOTAL_REVENUES"].shift(1)

    # Rimuove la prima riga che ha NaN in LAG_1
    df = df.dropna(subset=["LAG_1"]).reset_index(drop=True)

    # X = matrice delle feature, y = vettore target
    X = df[["MONTH_IDX", "ORDER_MONTH", "LAG_1"]].values
    y = df["TOTAL_REVENUES"].values

    print(f"Campioni totali disponibili: {len(X)}")
    print(f"Feature usate: MONTH_IDX, ORDER_MONTH, LAG_1")

    # Valutazione con TimeSeriesSplit (3 fold)
    tscv = TimeSeriesSplit(n_splits=3)

    mae_per_fold  = []
    rmse_per_fold = []
    r2_per_fold   = []

    print("\n  Risultati per fold:")
    print(f"  {'Fold':<6} {'Train':>8} {'Test':>6} {'MAE':>12} {'RMSE':>12} {'R²':>8}")
    print("  " + "-" * 56)

    for numero_fold, (indici_train, indici_test) in enumerate(tscv.split(X), start=1):

        X_train = X[indici_train]
        y_train = y[indici_train]
        X_test  = X[indici_test]
        y_test  = y[indici_test]

        modello_fold = LinearRegression()
        modello_fold.fit(X_train, y_train)

        y_test_previsto = modello_fold.predict(X_test)

        mae_fold  = mean_absolute_error(y_test, y_test_previsto)
        rmse_fold = np.sqrt(mean_squared_error(y_test, y_test_previsto))
        r2_fold   = r2_score(y_test, y_test_previsto)

        mae_per_fold.append(mae_fold)
        rmse_per_fold.append(rmse_fold)
        r2_per_fold.append(r2_fold)

        print(f"  {numero_fold:<6} {len(X_train):>8} {len(X_test):>6} "
              f"{mae_fold:>12,.0f} {rmse_fold:>12,.0f} {r2_fold:>8.4f}")

    mae_medio  = np.mean(mae_per_fold)
    rmse_medio = np.mean(rmse_per_fold)
    r2_medio   = np.mean(r2_per_fold)

    print(f"\n  Media sui 3 fold:")
    print(f"  MAE medio:  {mae_medio:,.0f}")
    print(f"  RMSE medio: {rmse_medio:,.0f}")
    print(f"  R² medio:   {r2_medio:.4f}")

    # Training finale su tutti i dati disponibili
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
# 3. addestra_modello_gbr
# =============================================================================

def addestra_modello_gbr(mensile):
    """
    Allena un modello GradientBoostingRegressor sui dati mensili aggregati.

    GradientBoosting costruisce in sequenza N alberi decisionali, ognuno dei
    quali corregge l'errore residuo del precedente. La previsione finale è la
    somma di tutte le correzioni scalate dal learning_rate.

    Parametri del modello:
        n_estimators  = 100  : numero di alberi
        max_depth     = 2    : profondità massima per limitare l'overfitting
        learning_rate = 0.1  : peso di ogni albero nella somma finale
        subsample     = 0.8  : frazione di campioni per albero (riduce overfitting)
        random_state  = 42   : seme per la riproducibilità dei risultati

    La valutazione usa TimeSeriesSplit con 3 fold, identica ad addestra_modello.
    Dopo la valutazione il modello viene riallenato su tutti i dati disponibili.

    Feature costruite internamente (stesse di addestra_modello):
        MONTH_IDX  : indice progressivo del mese → trend
        ORDER_MONTH: numero del mese (1-12) → stagionalità
        LAG_1      : ricavo del mese precedente → memoria a breve termine

    Parametri
    ----------
    mensile : DataFrame mensile (da aggrega_dati["mensile"])

    Restituisce
    -----------
    modello   : GradientBoostingRegressor allenato, pronto per predict()
    metriche  : dizionario con MAE, RMSE, R² medi sui fold + importanza feature
    """

    # Consolida per mese (somma su azienda, paese, business line)
    df = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        TOTAL_REVENUES = ("TOTAL_REVENUES", "sum"),
        N_ORDERS       = ("N_ORDERS",       "sum"),
    ).reset_index()

    df = df.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Indice progressivo del mese: 0 per il primo, 1 per il secondo, ecc.
    df["MONTH_IDX"] = range(len(df))

    # Ricavo del mese precedente
    df["LAG_1"] = df["TOTAL_REVENUES"].shift(1)

    # Rimuove la prima riga che ha NaN in LAG_1
    df = df.dropna(subset=["LAG_1"]).reset_index(drop=True)

    # X = matrice delle feature, y = vettore target
    X = df[["MONTH_IDX", "ORDER_MONTH", "LAG_1"]].values
    y = df["TOTAL_REVENUES"].values

    print(f"Campioni totali disponibili: {len(X)}")
    print(f"Feature usate: MONTH_IDX, ORDER_MONTH, LAG_1")
    print(f"Modello: GradientBoostingRegressor (n_estimators=100, max_depth=2, lr=0.1)")

    # Definizione del modello — max_depth=2 per limitare l'overfitting
    # con un dataset piccolo (14 campioni mensili)
    params_gbr = dict(
        n_estimators  = 100,
        max_depth     = 2,
        learning_rate = 0.1,
        subsample     = 0.8,
        random_state  = 42,
    )

    # Valutazione con TimeSeriesSplit (3 fold)
    tscv = TimeSeriesSplit(n_splits=3)

    mae_per_fold  = []
    rmse_per_fold = []
    r2_per_fold   = []

    print("\n  Risultati per fold:")
    print(f"  {'Fold':<6} {'Train':>8} {'Test':>6} {'MAE':>12} {'RMSE':>12} {'R²':>8}")
    print("  " + "-" * 56)

    for numero_fold, (indici_train, indici_test) in enumerate(tscv.split(X), start=1):

        X_train = X[indici_train]
        y_train = y[indici_train]
        X_test  = X[indici_test]
        y_test  = y[indici_test]

        # Ogni fold usa un modello fresco (nessuna memoria dei fold precedenti)
        modello_fold = GradientBoostingRegressor(**params_gbr)
        modello_fold.fit(X_train, y_train)

        y_test_previsto = modello_fold.predict(X_test)

        mae_fold  = mean_absolute_error(y_test, y_test_previsto)
        rmse_fold = np.sqrt(mean_squared_error(y_test, y_test_previsto))
        r2_fold   = r2_score(y_test, y_test_previsto)

        mae_per_fold.append(mae_fold)
        rmse_per_fold.append(rmse_fold)
        r2_per_fold.append(r2_fold)

        print(f"  {numero_fold:<6} {len(X_train):>8} {len(X_test):>6} "
              f"{mae_fold:>12,.0f} {rmse_fold:>12,.0f} {r2_fold:>8.4f}")

    mae_medio  = np.mean(mae_per_fold)
    rmse_medio = np.mean(rmse_per_fold)
    r2_medio   = np.mean(r2_per_fold)

    print(f"\n  Media sui 3 fold:")
    print(f"  MAE medio:  {mae_medio:,.0f}")
    print(f"  RMSE medio: {rmse_medio:,.0f}")
    print(f"  R² medio:   {r2_medio:.4f}")

    # Training finale su tutti i dati disponibili
    print(f"\n  Training finale su tutti i {len(X)} campioni...")
    modello = GradientBoostingRegressor(**params_gbr)
    modello.fit(X, y)

    # Importanza delle feature: quanto ogni feature ha contribuito agli alberi.
    # Disponibile solo in GradientBoosting, non nella regressione lineare.
    importanze = dict(zip(
        ["MONTH_IDX", "ORDER_MONTH", "LAG_1"],
        modello.feature_importances_
    ))
    print(f"\n  Importanza delle feature:")
    for nome, valore in sorted(importanze.items(), key=lambda x: -x[1]):
        barra = "█" * int(valore * 30)
        print(f"    {nome:<15} {valore:.3f}  {barra}")

    metriche = {
        "MAE_medio_cv":        mae_medio,
        "RMSE_medio_cv":       rmse_medio,
        "R2_medio_cv":         r2_medio,
        "mae_per_fold":        mae_per_fold,
        "r2_per_fold":         r2_per_fold,
        "feature_importances": importanze,
    }

    return modello, metriche


# =============================================================================
# 4. genera_previsioni
# =============================================================================

def genera_previsioni(modello, mensile, n_mesi=6):
    """
    Genera previsioni mensili per i prossimi N mesi.

    Per ogni mese futuro costruisce le tre feature (MONTH_IDX, ORDER_MONTH,
    LAG_1) e chiede al modello il ricavo previsto. Il valore previsto diventa
    il LAG_1 del mese successivo (strategia auto-regressiva): il modello usa
    le sue stesse previsioni come input per i passi successivi.

    Compatibile con qualsiasi modello scikit-learn che espone .predict(),
    quindi funziona sia con addestra_modello che con addestra_modello_gbr.

    Parametri
    ----------
    modello : modello scikit-learn già allenato (LinearRegression o GBR)
    mensile : DataFrame mensile (da aggrega_dati["mensile"])
    n_mesi  : numero di mesi futuri da prevedere (default 6)

    Restituisce
    -----------
    DataFrame con colonne ORDER_YEAR, ORDER_MONTH, DATE, FORECAST_REVENUES, TIPO.
    """

    # Consolida per mese per avere la serie storica pulita
    df = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        TOTAL_REVENUES = ("TOTAL_REVENUES", "sum"),
    ).reset_index()

    df = df.sort_values(["ORDER_YEAR", "ORDER_MONTH"]).reset_index(drop=True)

    # Punto di partenza: ultimo mese disponibile nello storico
    ultimo_idx    = len(df) - 1
    ultimo_anno   = int(df.iloc[-1]["ORDER_YEAR"])
    ultimo_mese   = int(df.iloc[-1]["ORDER_MONTH"])
    ultimo_ricavo = float(df.iloc[-1]["TOTAL_REVENUES"])

    righe_future = []

    for passo in range(1, n_mesi + 1):

        # Calcola mese e anno del passo corrente
        mese_new = (ultimo_mese + passo - 1) % 12 + 1
        anno_new = ultimo_anno + ((ultimo_mese + passo - 1) // 12)
        idx_new  = ultimo_idx + passo

        # Costruisce il vettore delle tre feature per questo mese futuro
        X_nuovo = np.array([[idx_new, mese_new, ultimo_ricavo]])

        # Previsione del modello; assicura che non sia negativa
        ricavo_previsto = float(modello.predict(X_nuovo)[0])
        ricavo_previsto = max(ricavo_previsto, 0)

        # La previsione diventa il LAG_1 del passo successivo
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


# =============================================================================
# Funzioni di supporto interne ai grafici (non esportate)
# =============================================================================

def _prepara_storico(mensile):
    """
    Consolida la tabella mensile per mese totale e restituisce
    un DataFrame con colonne DATE e REVENUES, ordinato per data.
    """

    storico = mensile.groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
        REVENUES = ("TOTAL_REVENUES", "sum"),
    ).reset_index()

    storico["DATE"] = pd.to_datetime(
        storico["ORDER_YEAR"].astype(str) + "-" +
        storico["ORDER_MONTH"].astype(str).str.zfill(2) + "-01"
    )

    storico = storico.sort_values("DATE").reset_index(drop=True)

    return storico


def _salva(fig, nome_file, cartella):
    """
    Salva la figura matplotlib come PNG e chiude la figura
    per liberare memoria.
    """

    os.makedirs(cartella, exist_ok=True)
    percorso = os.path.join(cartella, nome_file)
    fig.savefig(percorso, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"Grafico salvato: {percorso}")


# =============================================================================
# 5. grafico_forecast_singolo
# =============================================================================

def grafico_forecast_singolo(mensile, previsioni, nome_modello,
                              nome_file="forecast_singolo.png",
                              cartella=OUTPUT_DIR):
    """
    Genera il grafico con lo storico mensile e una sola serie di previsioni.

    Parametri
    ----------
    mensile      : DataFrame mensile aggregato (da aggrega_dati["mensile"])
    previsioni   : DataFrame con colonne DATE e FORECAST_REVENUES
                   (restituito da genera_previsioni)
    nome_modello : stringa che appare nella legenda e nel titolo
                   es. "Regressione lineare" oppure "GradientBoosting"
    nome_file    : nome del file PNG da salvare (default "forecast_singolo.png")
    cartella     : cartella di destinazione (default OUTPUT_DIR)
    """

    storico = _prepara_storico(mensile)

    df_plot = previsioni[["DATE", "FORECAST_REVENUES"]].copy()
    df_plot["DATE"] = pd.to_datetime(df_plot["DATE"])
    df_plot = df_plot.sort_values("DATE").reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(13, 5))

    # Linea storica: blu, cerchi pieni, linea continua
    ax.plot(storico["DATE"], storico["REVENUES"],
            "o-", color="#4472C4", linewidth=2, markersize=5,
            label="Storico mensile", zorder=10)

    # Linea previsioni: arancione, quadrati, linea tratteggiata
    ax.plot(df_plot["DATE"], df_plot["FORECAST_REVENUES"],
            "s--", color="#ED7D31", linewidth=2, markersize=7,
            label=f"Previsione — {nome_modello}")

    # Linea verticale di separazione storico / previsioni
    data_sep = storico["DATE"].iloc[-1]
    ax.axvline(x=data_sep, color="gray", linestyle=":", linewidth=1.5, alpha=0.7)
    ax.text(data_sep, ax.get_ylim()[1] * 0.97, "  previsione →",
            color="gray", fontsize=9)

    ax.set_title(f"Ricavi mensili: storico e previsioni — {nome_modello}",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Ricavi (€)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.35)
    plt.tight_layout()

    _salva(fig, nome_file, cartella)


# =============================================================================
# 6. grafico_confronto_modelli
# =============================================================================

def grafico_confronto_modelli(mensile, prev_lr, prev_gbr,
                               nome_file="confronto_modelli.png",
                               cartella=OUTPUT_DIR):
    """
    Genera il grafico che mette a confronto le previsioni di due modelli:
    Regressione lineare (arancione) e GradientBoosting (verde).

    Utile per presentare la scelta del modello e mostrare quanto differiscono
    le previsioni dei due approcci sullo stesso orizzonte temporale.

    Parametri
    ----------
    mensile   : DataFrame mensile aggregato (da aggrega_dati["mensile"])
    prev_lr   : DataFrame previsioni regressione lineare
                (colonne DATE e FORECAST_REVENUES)
    prev_gbr  : DataFrame previsioni GradientBoosting
                (colonne DATE e FORECAST_REVENUES)
    nome_file : nome del file PNG da salvare (default "confronto_modelli.png")
    cartella  : cartella di destinazione (default OUTPUT_DIR)
    """

    storico = _prepara_storico(mensile)

    def prepara(df_prev):
        df = df_prev[["DATE", "FORECAST_REVENUES"]].copy()
        df["DATE"] = pd.to_datetime(df["DATE"])
        return df.sort_values("DATE").reset_index(drop=True)

    df_lr  = prepara(prev_lr)
    df_gbr = prepara(prev_gbr)

    fig, ax = plt.subplots(figsize=(13, 5))

    # Linea storica: blu, cerchi pieni, linea continua
    ax.plot(storico["DATE"], storico["REVENUES"],
            "o-", color="#4472C4", linewidth=2, markersize=5,
            label="Storico mensile", zorder=10)

    # Regressione lineare: arancione, quadrati, linea tratteggiata
    ax.plot(df_lr["DATE"], df_lr["FORECAST_REVENUES"],
            "s--", color="#ED7D31", linewidth=2, markersize=7,
            label="Regressione lineare")

    # GradientBoosting: verde, triangoli, linea tratteggiata
    ax.plot(df_gbr["DATE"], df_gbr["FORECAST_REVENUES"],
            "^--", color="#70AD47", linewidth=2, markersize=7,
            label="GradientBoosting")

    # Linea verticale di separazione storico / previsioni
    data_sep = storico["DATE"].iloc[-1]
    ax.axvline(x=data_sep, color="gray", linestyle=":", linewidth=1.5, alpha=0.7)
    ax.text(data_sep, ax.get_ylim()[1] * 0.97, "  previsione →",
            color="gray", fontsize=9)

    ax.set_title("Ricavi mensili: confronto Regressione lineare vs GradientBoosting",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Ricavi (€)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.35)
    plt.tight_layout()

    _salva(fig, nome_file, cartella)


# =============================================================================
# 7. grafico_confronto_orizzonti
# =============================================================================

def grafico_confronto_orizzonti(mensile, previsioni_dict,
                                 nome_file="confronto_orizzonti.png",
                                 cartella=OUTPUT_DIR):
    """
    Genera il grafico che confronta previsioni su orizzonti temporali diversi.

    Ogni orizzonte è una linea tratteggiata con colore e marcatore distinto.
    Utile per mostrare come le previsioni diventano meno affidabili
    man mano che ci si allontana dallo storico.

    Parametri
    ----------
    mensile         : DataFrame mensile aggregato (da aggrega_dati["mensile"])
    previsioni_dict : dizionario {etichetta: DataFrame} dove ogni DataFrame
                      ha le colonne DATE e FORECAST_REVENUES.

                      Esempio:
                          {
                              "6 mesi":  prev_6mesi,
                              "12 mesi": prev_12mesi,
                          }

    nome_file       : nome del file PNG da salvare (default "confronto_orizzonti.png")
    cartella        : cartella di destinazione (default OUTPUT_DIR)
    """

    # Palette colori e marcatori per gli orizzonti (ciclici se > 5 serie)
    colori    = ["#ED7D31", "#70AD47", "#FF0000", "#7030A0", "#00B0F0"]
    marcatori = ["s", "^", "D", "v", "P"]

    storico = _prepara_storico(mensile)

    fig, ax = plt.subplots(figsize=(13, 5))

    # Linea storica: blu, cerchi pieni, linea continua
    ax.plot(storico["DATE"], storico["REVENUES"],
            "o-", color="#4472C4", linewidth=2, markersize=5,
            label="Storico mensile", zorder=10)

    # Una linea tratteggiata per ogni orizzonte
    for indice, (etichetta, df_prev) in enumerate(previsioni_dict.items()):

        colore    = colori[indice % len(colori)]
        marcatore = marcatori[indice % len(marcatori)]

        df_plot = df_prev[["DATE", "FORECAST_REVENUES"]].copy()
        df_plot["DATE"] = pd.to_datetime(df_plot["DATE"])
        df_plot = df_plot.sort_values("DATE").reset_index(drop=True)

        ax.plot(df_plot["DATE"], df_plot["FORECAST_REVENUES"],
                marker=marcatore, linestyle="--", color=colore,
                linewidth=2, markersize=7, label=etichetta)

    # Linea verticale di separazione storico / previsioni
    data_sep = storico["DATE"].iloc[-1]
    ax.axvline(x=data_sep, color="gray", linestyle=":", linewidth=1.5, alpha=0.7)
    ax.text(data_sep, ax.get_ylim()[1] * 0.97, "  previsione →",
            color="gray", fontsize=9)

    ax.set_title("Ricavi mensili: confronto orizzonti di previsione",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Ricavi (€)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.35)
    plt.tight_layout()

    _salva(fig, nome_file, cartella)