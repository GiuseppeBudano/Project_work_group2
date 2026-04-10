# =============================
# --- IMPORTAZIONE LIBRERIE ---
# =============================

import pandas as pd
import logging
import os
from datetime import datetime

# ================
# --- LOGGER ---
# ================

# 1. Definizione livello OK
LIVELLO_OK = 25
logging.addLevelName(LIVELLO_OK, "OK")

def ok(self, message, *args, **kwargs):
    """
    Registra un messaggio al livello personalizzato OK (25).
    """
    if self.isEnabledFor(LIVELLO_OK):
        self._log(LIVELLO_OK, message, args, **kwargs)

logging.Logger.ok = ok

# 2. Creazione logger globale
def crea_logger(livello=logging.DEBUG):
    """
    Inizializza il logger dell’ETL creando un file nella cartella 'logs'
    con timestamp e formato standard. Restituisce un logger configurato
    al livello indicato.
    """
    # Cartella logs
    cartella_log = "logs"
    os.makedirs(cartella_log, exist_ok=True)

    # Nome file con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_file = os.path.join(cartella_log, f"etl_{timestamp}.log")

    # Logger
    logger = logging.getLogger("etl")
    logger.setLevel(livello)

    # Evita duplicazioni in Streamlit/Jupyter
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.FileHandler(nome_file)
    handler.setLevel(livello)

    formato = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formato)

    logger.addHandler(handler)

    logger.info("Logger inizializzato correttamente (etl.py::crea_logger)")
    return logger

# 3. Logger inizializzato
logger = crea_logger()
logger.info("=== AVVIO ETL (etl.py::main) ===")

# =============================
# --- CARICAMENTO FILES CSV ---
# =============================

def carica_tabelle_csv():
    """
    Carica tutte le tabelle CSV necessarie al dataset iniziale.
    Restituisce un dizionario {nome_tabella: DataFrame} oppure None in caso di errori.
    """
    # Dizionario dei file da caricare
    file_paths = {
                  "SALES": "SALES.csv",
                  "AREA_MANAGER_LOOKUP": "AREA_MANAGER_LOOKUP.csv",
                  "COMPANY_LOOKUP": "COMPANY_LOOKUP.csv",
                  "CUSTOMER_LOOKUP": "CUSTOMER_LOOKUP.csv",
                  "ITEM_BUSINESS_LINE_LOOKUP": "ITEM_BUSINESS_LINE_LOOKUP.csv",
                  "ITEM_LOOKUP": "ITEM_LOOKUP.csv"
                 }
    tabelle = {}
    tabelle_non_caricate = []
    
    for nome_tab, path in file_paths.items():
        logger.info(f"[carica_tabelle_csv] Caricamento tabella '{nome_tab}' dal file '{path}'")

        try:
            # Tentativo di lettura del file CSV
            df = pd.read_csv(path)
            righe, colonne = df.shape
        
            # Controllo tabella vuota
            if righe == 0:
                logger.error(f"[carica_tabelle_csv] Il file '{path}' è vuoto.")
                tabelle_non_caricate.append(nome_tab)
                continue

            # Stampa informazioni sulla tabella caricata
            logger.ok(f"[carica_tabelle_csv] Tabella {nome_tab} caricata correttamente ({righe} righe, {colonne} colonne)")
            tabelle[nome_tab] = df

        except Exception as e:
            # Gestione errori di lettura (file mancante, formati errati, permessi, ...)
            logger.error(f"[carica_tabelle_csv] Impossibile leggere il file '{path}': {e}")
            tabelle_non_caricate.append(nome_tab)
            
    # Controllo finale
    if tabelle_non_caricate:
        logger.error(f"[carica_tabelle_csv] Tabelle non caricate: {tabelle_non_caricate}")
        return None
    
    logger.ok("[carica_tabelle_csv] Tutte le tabelle sono state caricate correttamente")
    
    return tabelle

# ============================
# --- CONTROLLO DUPLICATI ---
# ============================

def rimuovi_duplicati(tabelle_dict):
    """
    Elimina le righe duplicate, se 
    """
    for nome, df in tabelle_dict.items():
        df = tabelle_dict[nome]
        #righe prima della pulizia
        righe_iniziali = df.shape[0]
        #rimozione dei duplicati
        df_pulito = df.drop_duplicates(keep="first").copy()
        #righe dopo la pulizia
        righe_finali = df_pulito.shape[0]
        #calcolo delle righe eliminate
        eliminate = righe_iniziali - righe_finali

        print(f"[ATTENZIONE] Nella tabella '{nome}' sono state eliminate {eliminate} righe duplicate.")

        #Aggiornare il dizionario delle tabelle con la tabella pulita
        tabelle_dict[nome] = df_pulito

    return tabelle_dict

# ========================
# --- CONTROLLO CHIAVI ---
# ========================

def configura_chiavi():
    """
    Restituisce i dizionari di configurazione:
    - relazioni FK
    - chiavi primarie
    - chiavi tecniche
    """

    relazioni = {
        "SALES": {
            "ID_COMPANY": "COMPANY_LOOKUP",
            "IDS_CUSTOMER": "CUSTOMER_LOOKUP",
            "IDS_ITEM": "ITEM_LOOKUP"
        },
        "CUSTOMER_LOOKUP": {
            "ID_AREA_MANAGER": "AREA_MANAGER_LOOKUP"
        },
        "ITEM_LOOKUP": {
            "ID_BUSINESS_LINE": "ITEM_BUSINESS_LINE_LOOKUP"
        }
    }

    chiavi_pk = {
        "SALES": [],
        "CUSTOMER_LOOKUP": ["IDS_CUSTOMER"],
        "ITEM_LOOKUP": ["IDS_ITEM"],
        "AREA_MANAGER_LOOKUP": ["ID_AREA_MANAGER"],
        "ITEM_BUSINESS_LINE_LOOKUP": ["ID_BUSINESS_LINE"],
        "COMPANY_LOOKUP": ["ID_COMPANY"]
    }

    chiavi_tecniche = {
        "SALES": ["ID_ORDER_NUM", "ID_ORDER_DATE", "ID_INVOICE_DATE"],
        "CUSTOMER_LOOKUP": ["ID_COUNTRY"]
    }

    return relazioni, chiavi_pk, chiavi_tecniche

def controlla_pk(tabella, df, lista_pk):

    for pk in lista_pk:
        print(f"\n[INFO] Analisi della PK: {pk} per la tabella: {tabella}")

        if pk not in df.columns:
            print(f"[ERRORE] La colonna PK '{pk}' NON esiste nella tabella {tabella}.")
            continue
        else:
            print(f"[OK] La colonna PK '{pk}' esiste.")

        serie = df[pk]

        # NaN
        num_nan = serie.isna().sum()
        if num_nan == 0:
            print(f"[OK] Nessun NaN nella PK '{pk}'.")
        else:
            print(f"[ERRORE] La PK '{pk}' contiene {num_nan} valori NaN.")

        # Duplicati
        duplicati = serie[serie.duplicated()]
        if len(duplicati) == 0:
            print(f"[OK] La PK '{pk}' è univoca.")
        else:
            print(f"[ERRORE] La PK '{pk}' ha {len(duplicati)} duplicati.")

        # Tipi coerenti
        tipi_presenti = serie.dropna().map(type).unique()
        if len(tipi_presenti) == 1:
            print(f"[OK] Tipo coerente: {tipi_presenti[0].__name__}")
        else:
            print(f"[ERRORE] Tipi NON coerenti: {tipi_presenti}")

def controlla_fk(tabella_figlia, df_figlia, fk, tabella_padre, df_padre, pk):
    print(f"\n=== Controllo FK {tabella_figlia}.{fk} → {tabella_padre}.{pk} ===")

    # Esistenza colonne
    if fk not in df_figlia.columns:
        print(f"[ERRORE] La colonna FK '{fk}' non esiste nella tabella figlia {tabella_figlia}.")
        return
    if pk not in df_padre.columns:
        print(f"[ERRORE] La colonna PK '{pk}' non esiste nella tabella padre {tabella_padre}.")
        return

    col_fk = df_figlia[fk]
    col_pk = df_padre[pk]

    # NaN
    num_nan = col_fk.isna().sum()
    if num_nan > 0:
        print(f"[INFO] La FK '{fk}' contiene {num_nan} valori NaN (relazione opzionale).")
    else:
        print(f"[OK] Nessun NaN nella FK '{fk}'.")

    # Tipo coerente
    if col_fk.dtype != col_pk.dtype:
        print(f"[ATTENZIONE] Tipi diversi: FK={col_fk.dtype}, PK={col_pk.dtype}")
    else:
        print(f"[OK] Tipi coerenti tra FK e PK.")

    # Valori orfani
    valori_fk = set(col_fk.dropna().unique())
    valori_pk = set(col_pk.dropna().unique())
    orfani = valori_fk - valori_pk

    if len(orfani) > 0:
        print(f"[ERRORE] Valori orfani trovati: {len(orfani)}")
    else:
        print("[OK] Nessun valore orfano.")

    # Cardinalità 1:N (default)
    print("[OK] Cardinalità 1:N (duplicati ammessi).")

def controlla_chiavi_tecniche(tabella, df, lista_chiavi_tecniche):

    for chiave in lista_chiavi_tecniche:
        print(f"\n[INFO] Analisi chiave tecnica: {chiave}")

        # Esistenza colonna
        if chiave not in df.columns:
            print(f"[ERRORE] La colonna tecnica '{chiave}' NON esiste nella tabella {tabella}.")
            continue
        else:
            print(f"[OK] La colonna tecnica '{chiave}' esiste.")

        serie = df[chiave]

        # NaN
        num_nan = serie.isna().sum()
        if num_nan > 0:
            print(f"[ERRORE] La chiave tecnica '{chiave}' contiene {num_nan} valori NaN.")
            print(f"[AZIONE] Eliminazione delle righe con NaN nella colonna '{chiave}'.")
            df.dropna(subset=[chiave], inplace=True)
        else:
            print(f"[OK] Nessun NaN nella chiave tecnica '{chiave}'.")

        # Tipi coerenti
        tipi_presenti = serie.dropna().map(type).unique()
        if len(tipi_presenti) == 1:
            print(f"[OK] Tipo coerente: {tipi_presenti[0].__name__}")
        else:
            print(f"[ATTENZIONE] Tipi NON coerenti: {tipi_presenti}")

    return df

def controlla_chiavi(dizionario_tabelle, relazioni, chiavi_pk, chiavi_tecniche):
    # --- PK ---
    print("\n=== [controlla_chiavi] FASE 3.1: CONTROLLO CHIAVI PRIMARIE ===")
    for tabella, lista_pk in chiavi_pk.items():
        df = dizionario_tabelle[tabella]
        controlla_pk(tabella, df, lista_pk)

    # --- FK ---
    print("\n=== [controlla_chiavi] FASE 3.2: CONTROLLO CHIAVI ESTERNE ===")
    for tabella_figlia, mapping in relazioni.items():
        df_figlia = dizionario_tabelle[tabella_figlia]

        for fk, tabella_padre in mapping.items():
            df_padre = dizionario_tabelle[tabella_padre]
            pk_padre = chiavi_pk[tabella_padre][0]  # assumiamo PK singola
            controlla_fk(tabella_figlia, df_figlia, fk, tabella_padre, df_padre, pk_padre)

    # --- CHIAVI TECNICHE ---
    print("\n=== [controlla_chiavi] FASE 3.3: CONTROLLO CHIAVI TECNICHE ===")
    for tabella, lista_chiavi in chiavi_tecniche.items():
        df = dizionario_tabelle[tabella]
        df = controlla_chiavi_tecniche(tabella, df, lista_chiavi)
        dizionario_tabelle[tabella] = df
        
# ========================
# --- AGGIUNTA MARGINI ---
# ========================

def calcola_margini(df, nome_tabella="SALES"):
    """
    Calcola la colonna VAL_MARGIN = VAL_REVENUES - VAL_COST.
    Restituisce il dataframe aggiornato e stampa l'esito dell'operazione.
    """
    
    colonne_richieste = ["VAL_REVENUES", "VAL_COST"]
    mancanti = []
    for c in colonne_richieste:
        if c not in df.columns:
            mancanti.append(c)

    # Controllo colonne richieste
    if mancanti:
        print(f"[ERRORE] Impossibile calcolare VAL_MARGIN nella tabella {nome_tabella}: "
              f"colonne mancanti: {mancanti}")
        return df

    # Calcolo margini con gestione errori
    try:
        df["VAL_MARGIN"] = df["VAL_REVENUES"] - df["VAL_COST"]
        print(f"[OK] Colonna VAL_MARGIN calcolata correttamente nella tabella {nome_tabella}.")
    except Exception as e:
        print(f"[ERRORE] Errore durante il calcolo di VAL_MARGIN nella tabella {nome_tabella}: {e}")

    return df

# =============
# --- MERGE ---
# =============

def merge_tabelle(df, tabella_figlia, relazioni, dizionario_tabelle):
    """
    Esegue il merge ricorsivo delle tabelle collegate tramite relazioni.
    df = DataFrame di partenza
    tabella_figlia = nome della tabella da cui partire
    relazioni = dizionario {tabella_figlia: {fk: tabella_padre}}
    dizionario_tabelle = tutte le tabelle caricate
    """

    if tabella_figlia not in relazioni:
        return df

    for fk, tabella_padre in relazioni[tabella_figlia].items():

        # Controllo esistenza colonne
        if fk not in df.columns:
            print(f"[ERRORE] La colonna FK '{fk}' non esiste nella tabella {tabella_figlia}. Merge impossibile.")
            continue

        df_padre = dizionario_tabelle[tabella_padre]

        if fk not in df_padre.columns:
            print(f"[ERRORE] La colonna PK '{fk}' non esiste nella tabella padre {tabella_padre}. Merge impossibile.")
            continue

        # Righe prima del merge
        righe_prima = len(df)

        print(f"\n[INFO] Avvio merge LEFT JOIN tra {tabella_figlia} e {tabella_padre} sulla chiave '{fk}'.")
        print(f"[INFO] Righe tabella figlia: {len(df)} | Righe tabella padre: {len(df_padre)}")

        try:
            df = df.merge(
                df_padre,
                how="left",
                left_on=fk,
                right_on=fk,
                suffixes=("", f"_{tabella_padre}")
            )
        except Exception as e:
            print(f"[ERRORE] Merge fallito tra {tabella_figlia} e {tabella_padre}: {e}")
            continue

        # Righe dopo il merge
        righe_dopo = len(df)

        # Log esito
        if righe_dopo == righe_prima:
            print(f"[OK] Merge completato senza variazioni nel numero di righe ({righe_dopo}).")
        elif righe_dopo > righe_prima:
            print(f"[ATTENZIONE] Il merge ha aumentato le righe: {righe_prima} → {righe_dopo}.")
            print("           Possibile duplicazione dovuta a chiavi non univoche nella tabella padre.")
        else:
            print(f"[ATTENZIONE] Il merge ha RIDOTTO le righe: {righe_prima} → {righe_dopo}.")
            print("           Questo NON dovrebbe accadere con un LEFT JOIN. Verificare i dati.")

        # Ricorsione
        df = merge_tabelle(df, tabella_padre, relazioni, dizionario_tabelle)

    return df

# =====================
# --- CONTROLLO NAN ---
# =====================

def trovare_nan(df):
    """
    Scorre tutte le colonne del DataFrame, individua quelle con NaN
    e stampa il numero di NaN per ciascuna.
    Restituisce la lista delle colonne che contengono NaN.
    """
    colonne_nan = []

    for col in df.columns:
        num_nan = df[col].isna().sum()
        if num_nan > 0:
            colonne_nan.append(col)
            print(f"[ATTENZIONE] la colonna {col} contiene: {num_nan} NaN")

    if not colonne_nan:
        print("[OK] Nessuna colonna contiene NaN.")

    return colonne_nan

# ============================
# --- NORMALIZZAZIONE DATE ---
# ============================

def analizza_colonne_date(df):
    """
    Analizza tutte le colonne che contengono 'DATE' nel nome.
    Stampa:
    - numero di valori a 8 cifre
    - numero di valori NON a 8 cifre
    - eventuali valori con anno > 2262
    Restituisce una lista delle colonne che contengono 'DATE'.
    """
    #Trova tutte le colonne che contengono "DATE"
    colonne_date = []
    for col in df.columns:
        if "DATE" in col.upper():
            colonne_date.append(col)
    #Se non ci sono colonne DATE, termina
    if not colonne_date:
        print("[INFO] Nessuna colonna contenente 'DATE' trovata.")
        return []

    #Elaborazione di ogni colonna DATE
    for col in colonne_date:
        #Conversione preliminare a stringa
        serie = df[col].astype(str).str.strip()
        #Identifica i valori che hanno esattamente 8 cifre
        mask_8 = serie.str.match(r"^\d{8}$")
        valori_8 = serie[mask_8]
        #Identifica i valori che non hanno esattamente 8 cifre
        mask_non_8 = serie.str.match(r"^\d{8}$") == False
        valori_non_8 = serie[mask_non_8]
        if len(valori_8) > 0:
            print(f"[OK] La colonna '{col}' contiene {len(valori_8)} valori a 8 cifre.")
        else:
            print(f"[ATTENZIONE] La colonna '{col}' contiene {len(valori_non_8)} valori NON a 8 cifre.")

        #Individua i valori con anno fuori dal range pandas
        fuori_range = []
        for x in valori_8:
            anno = int(x[:4])
            if anno > 2262:
                fuori_range.append(x)
        #Se esistono valori fuori range, stampa un warning
        if fuori_range:
            valori_unici_fuori_range = sorted(set(fuori_range))
            print(f"[ATTENZIONE] {len(fuori_range)} valori con anno fuori range pandas → saranno convertiti in stringhe.")
            print(f"[INFO] Valori fuori range trovati: {valori_unici_fuori_range[:5]}")

    return colonne_date

def normalizza_colonne_date(df, colonne_date):
    """
    Normalizza le colonne DATE passate come lista.
    Restituisce un DataFrame aggiornato.
    """
    if not colonne_date:
        print("[INFO] Nessuna colonna DATE da normalizzare.")
        return df

    for col in colonne_date:
        serie = df[col].astype(str).str.strip()
        nuova_colonna = []
        
        for x in serie:
            
            #Caso: formato YYYYMMDD
            if len(x) == 8 and x.isdigit():
                anno = int(x[:4])
                mese = int(x[4:6])
                giorno = int(x[6:8])
                #Caso speciale: anno fuori range pandas
                if anno > 2262:
                    nuova_colonna.append(f"{anno:04d}-{mese:02d}-{giorno:02d}")
                    continue

                #Conversione normale
                try:
                    dt = pd.to_datetime(x, format="%Y%m%d", errors="coerce")
                    if pd.isna(dt):
                        nuova_colonna.append(None)
                    else:
                        nuova_colonna.append(dt.strftime("%Y-%m-%d"))
                except:
                    nuova_colonna.append(None)

            else:
                #Fallback per formati non a 8 cifre
                try:
                    dt = pd.to_datetime(x, errors="coerce")
                    if pd.isna(dt):
                        nuova_colonna.append(None)
                    else:
                        nuova_colonna.append(dt.strftime("%Y-%m-%d"))
                except:
                    nuova_colonna.append(None)

        #Sostituisci la colonna nel DataFrame
        df[col] = nuova_colonna

        #Report finale
        num_nat = df[col].isna().sum()
        print(f"[OK] Conversione completata per '{col}'.")
        print(f"[INFO] Valori NaT finali: {num_nat}")
        
    return df

# ======================================
# --- AGGIUNTA ANNO, MESE, SETTIMANA ---
# ======================================

def aggiungi_order_features(df):
    """
    Aggiunge:
    - ORDER_YEAR  (anno dell'ordine)
    - ORDER_MONTH  (mese dell'ordine)
    - ORDER_WEEK  (settimana ISO dell'ordine)
    Richiede che ID_ORDER_DATE sia già in formato datetime.
    """
    #Controllo esistenza colonna
    if "ID_ORDER_DATE" not in df.columns:
        print("[ERRORE] La colonna 'ID_ORDER_DATE' non esiste nel DataFrame.")
        return df

    #Controllo tipo
    if not pd.api.types.is_datetime64_any_dtype(df["ID_ORDER_DATE"]):
        print("[INFO] 'ID_ORDER_DATE' non è datetime. Conversione in corso...")
        try:
            df["ID_ORDER_DATE"] = pd.to_datetime(df["ID_ORDER_DATE"], errors="coerce")
            print("[OK] Conversione completata.")
        except Exception as e:
            print(f"[ERRORE] Conversione fallita: {e}")
            return df

    #Creazione colonne derivate
    df["ORDER_YEAR"] = df["ID_ORDER_DATE"].dt.year
    df["ORDER_MONTH"] = df["ID_ORDER_DATE"].dt.month
    df["ORDER_WEEK"] = df["ID_ORDER_DATE"].dt.isocalendar().week

    print("[OK] Colonne ORDER_YEAR, ORDER_MONTH e ORDER_WEEK create correttamente.")

    return df

# ========================
# --- SALVATAGGIO OLAP ---
# ========================

def salva_dataset_olap(df, nome_file="olap.csv"):
    """
    Salva il file OLAP finale in formato CSV e stampa un riepilogo.
    Tutta la logica è interna: il main deve solo chiamare questa funzione.
    """
    # Salvataggio del file
    df.to_csv(nome_file, index=False, encoding="utf-8")

    # Informazioni sul file salvato
    righe, colonne = df.shape
    print(f"[OK] File OLAP salvato correttamente: {nome_file}")
    print(f"[INFO] Dimensioni: {righe} righe, {colonne} colonne")

# ============
# --- MAIN ---
# ============

def esegui_etl(carica=True, salva=True, tabelle=None):
    # Caricamento dei file .csv
    logger.info("\n=== [main] FASE 1: DOWNLOAD DEL FILE OLAP ===")

    # Caso 1: passaggio delle tabelle da Streamlit
    if tabelle is not None:
        logger.ok("[main] Tabelle ricevute da Streamlit. Salto carica_tabelle_csv().")
    
    # Caso 2: utilizzo da Notebook, anche senza Stremlit le tabelle vengono caricate
    elif carica:
        logger.info("[main] Nessuna tabella fornita. Avvio carica_tabelle_csv().")
        tabelle = carica_tabelle_csv()
        if tabelle is None:
            logger.error("[main] Interruzione ETL: dataset sorgente non disponibile.")
            return None
            
    # Caso 3: carica=False e tabelle=None → errore logico
    else:
        logger.error("[main] carica=False ma nessuna tabella fornita. ETL impossibile.")
        return None

    # Validazione nomi attesi per le tabelle caricate
    nomi_attesi = {
        "SALES",
        "AREA_MANAGER_LOOKUP",
        "COMPANY_LOOKUP",
        "CUSTOMER_LOOKUP",
        "ITEM_BUSINESS_LINE_LOOKUP",
        "ITEM_LOOKUP"
    }

    mancanti = nomi_attesi - set(tabelle.keys())
    if mancanti:
        logger.error(f"[main] Tabelle mancanti: {mancanti}")
        return None

    logger.ok("[main] Tutte le tabelle richieste sono presenti.")

    # Eliminazione delle righe duplicate dalle tabelle
    print("\n=== [main] FASE 2: RIMOZIONE DUPLICATI ===")
    tabelle = rimuovi_duplicati(tabelle)

    # Controllo delle chiavi primarie, esterne e tecniche
    print("\n=== [main] FASE 3: CONTROLLO CHIAVI ===")
    relazioni, chiavi_pk, chiavi_tecniche = configura_chiavi()
    controlla_chiavi(tabelle, relazioni, chiavi_pk, chiavi_tecniche)
    
    # Arricchimento dati: aggiunta della colonna MARGINI
    print("\n=== [main] FASE 4: CALCOLO COLONNE DERIVATE – MARGINI ===")
    df_sales = tabelle["SALES"]
    df_sales = calcola_margini(df_sales, nome_tabella="SALES")
    tabelle["SALES"] = df_sales

    # Merge
    print("\n=== [main] FASE 5: MERGE TABELLE ===")
    df_merge = tabelle["SALES"].copy()
    df_merge = merge_tabelle(df_merge, "SALES", relazioni, tabelle)

    # Data cleaning: NaN
    print("\n=== [main] FASE 6: CONTROLLO NAN ===")
    colonne_con_nan = trovare_nan(df_merge)
    
    # Normalizzazione colonne DATE
    print("\n===== [main] FASE 7.1: ANALISI COLONNE DATA =====")
    colonne_data = analizza_colonne_date(df_merge)
    print("\n===== [main] FASE 7.2: NORMALIZZAZIONE COLONNE DATA =====")
    df_merge = normalizza_colonne_date(df_merge, colonne_data)
    
    # Aggiunta colonne ORDER_YEAR, ORDER_MESE, ORDER_WEEK
    print("\n===== [main] FASE 8: CREAZIONE ATTRIBUTI TEMPORALI =====")
    df_merge = aggiungi_order_features(df_merge)
    
    # Salvataggio del file OLAP finale SOLO se salva=True
    if salva:
        print("\n=== [main] FASE 9: DOWNLOAD DEL FILE OLAP ===")
        salva_dataset_olap(df_merge, nome_file="olap.csv")

    return df_merge

if __name__ == "__main__":
    esegui_etl()
