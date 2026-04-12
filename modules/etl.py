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

    handler = logging.FileHandler(nome_file, encoding="utf-8")
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

def prepara_tabelle(carica, tabelle):
    """
    Gestisce la logica di caricamento delle tabelle:
    - se tabelle è fornito (Streamlit), le usa
    - se carica=True, carica i CSV
    - altrimenti segnala errore logico
    Restituisce il dizionario tabelle oppure None.
    """
    # Caso 1: tabelle già fornite (Streamlit)
    if tabelle is not None:
        logger.ok("[main] Tabelle ricevute da Streamlit. Salto carica_tabelle_csv().")
        return tabelle

    # Caso 2: Notebook o esecuzione standard
    if carica:
        logger.info("[main] Nessuna tabella fornita. Avvio carica_tabelle_csv().")
        tabelle = carica_tabelle_csv()
        if tabelle is None:
            logger.error("[main] Interruzione ETL: dataset sorgente non disponibile.")
            return None
        return tabelle

    # Caso 3: errore logico
    logger.error("[main] carica=False ma nessuna tabella fornita. ETL impossibile.")
    return None

def verifica_tabelle_attese(tabelle):
    """
    Verifica che tutte le tabelle richieste siano presenti.
    Restituisce True se OK, False altrimenti.
    """
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
        return False

    logger.ok("[main] Tutte le tabelle richieste sono presenti.")
    return True

# ============================
# --- CONTROLLO DUPLICATI ---
# ============================

def rimuovi_duplicati(tabelle_dict):
    """
    Elimina le righe duplicate da ciascuna tabella del dizionario.
    Logga il numero di righe eliminate per ogni tabella. 
    """
    for nome, df in tabelle_dict.items():
        logger.info(f"[rimuovi_duplicati] Controllo dei duplicati nella tabella {nome}")
        df = tabelle_dict[nome]
        # Righe prima della pulizia
        righe_iniziali = df.shape[0]
        # Rimozione dei duplicati
        df_pulito = df.drop_duplicates(keep="first").copy()
        # Righe dopo la pulizia
        righe_finali = df_pulito.shape[0]
        # Calcolo delle righe eliminate
        eliminate = righe_iniziali - righe_finali

        logger.info(f"Nella tabella '{nome}' sono state eliminate {eliminate} righe duplicate.")

        # Aggiornare il dizionario delle tabelle con la tabella pulita
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
    # Relazioni dataset
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
    # Chiavi primarie
    chiavi_pk = {
        "SALES": [],
        "CUSTOMER_LOOKUP": ["IDS_CUSTOMER"],
        "ITEM_LOOKUP": ["IDS_ITEM"],
        "AREA_MANAGER_LOOKUP": ["ID_AREA_MANAGER"],
        "ITEM_BUSINESS_LINE_LOOKUP": ["ID_BUSINESS_LINE"],
        "COMPANY_LOOKUP": ["ID_COMPANY"]
    }
    # Chiavi tecniche = non relazionali
    chiavi_tecniche = {
        "SALES": ["ID_ORDER_NUM", "ID_ORDER_DATE", "ID_INVOICE_DATE"],
        "CUSTOMER_LOOKUP": ["ID_COUNTRY"]
    }

    return relazioni, chiavi_pk, chiavi_tecniche

def controlla_pk(tabella, df, lista_pk):
    """
    Controlla l'integrità delle chiavi primarie:
    - verifica esistenza colonna PK
    - converte il tipo di dato in stringa
    - segnala e rimuove righe con NaN nella PK
    - segnala e rimuove righe con duplicati nella PK
    - verifica coerenza del tipo dati
    Restituisce il DataFrame aggiornato.
    """
    for pk in lista_pk:
        logger.info(f"\n--- [controlla_pk] Controllo PK: {pk} per la tabella: {tabella} ---")

        if pk not in df.columns:
            logger.error(f"La colonna PK '{pk}' NON esiste nella tabella {tabella}.")
            continue

        logger.ok(f"La colonna PK '{pk}' esiste.")

        # Conversione PK a stringa
        df[pk] = df[pk].astype(str)
        logger.ok(f"La PK '{pk}' è stata convertita a stringa.")
        
        serie = df[pk]

        # NaN
        num_nan = serie.isna().sum()
        if num_nan > 0:
            logger.error(f"La PK '{pk}' contiene {num_nan} valori NaN.")
            logger.info(f"Eliminazione delle righe con NaN in '{pk}'.")
            df = df.dropna(subset=[pk])
            serie = df[pk]
        else:
            logger.ok(f"Nessun NaN nella PK '{pk}'.")

        # Duplicati
        duplicati = serie[serie.duplicated()]
        if len(duplicati) > 0:
            logger.error(f"La PK '{pk}' ha {len(duplicati)} duplicati.")
            logger.info(f"Eliminazione delle righe duplicate sulla PK '{pk}'.")
            df = df.drop_duplicates(subset=[pk])
            serie = df[pk]
        else:
            logger.ok(f"La PK '{pk}' è univoca.")

        # Tipi coerenti
        tipi_presenti = serie.dropna().map(type).unique()
        if len(tipi_presenti) == 1:
            logger.ok(f"Tipo coerente: {tipi_presenti[0].__name__}")
        else:
            logger.error(f"Tipi NON coerenti: {tipi_presenti}")

    return df

def controlla_fk(tabella_figlia, df_figlia, fk, tabella_padre, df_padre, pk):
    """
    Controlla l'integrità della foreign key:
    - verifica esistenza FK e PK
    - converte FK e PK a stringa
    - elimina righe con NaN nella FK
    - elimina duplicati sulla FK
    - elimina righe con valori orfani (FK non presenti nella PK padre)
    - verifica coerenza del tipo dati
    Restituisce il DataFrame figlio aggiornato.
    """
    logger.info(f"\n--- [controlla_fk] Controllo FK {tabella_figlia}.{fk} --> {tabella_padre}.{pk} ---")

    # Esistenza colonne
    if fk not in df_figlia.columns:
        logger.error(f"La colonna FK '{fk}' non esiste nella tabella figlia {tabella_figlia}.")
        return df_figlia

    if pk not in df_padre.columns:
        logger.error(f"La colonna PK '{pk}' non esiste nella tabella padre {tabella_padre}.")
        return df_figlia

    # Conversione FK e PK a stringa
    df_figlia[fk] = df_figlia[fk].astype(str)
    df_padre[pk] = df_padre[pk].astype(str)
    logger.ok(f"Convertite a stringa FK '{fk}' e PK '{pk}'.")

    col_fk = df_figlia[fk]
    col_pk = df_padre[pk]

    # NaN
    num_nan = col_fk.isna().sum()
    if num_nan > 0:
        logger.error(f"La FK '{fk}' contiene {num_nan} valori NaN.")
        logger.info(f"Eliminazione delle righe con NaN in '{fk}'.")
        df_figlia = df_figlia.dropna(subset=[fk])
        col_fk = df_figlia[fk]
    else:
        logger.ok(f"Nessun NaN nella FK '{fk}'.")

    # Valori orfani
    valori_fk = set(col_fk.dropna().unique())
    valori_pk = set(df_padre[pk].dropna().unique())
    orfani = valori_fk - valori_pk

    if len(orfani) > 0:
        logger.error(f"Valori orfani trovati: {len(orfani)}")
        logger.info("Eliminazione delle righe con FK orfane.")
        df_figlia = df_figlia[~df_figlia[fk].isin(orfani)]
    else:
        logger.ok("Nessun valore orfano.")

    # Tipo coerente
    if df_figlia[fk].dtype != df_padre[pk].dtype:
        logger.warning(f"Tipi diversi: FK={df_figlia[fk].dtype}, PK={df_padre[pk].dtype}")
    else:
        logger.ok("Tipi coerenti tra FK e PK.")

    return df_figlia

def controlla_chiavi_tecniche(tabella, df, lista_chiavi_tecniche):
    """
    Esegue i controlli di integrità sulle chiavi del modello:
    - chiavi primarie: esistenza, NaN, duplicati, tipi coerenti
    - chiavi esterne: esistenza, NaN, duplicati, orfani, tipi coerenti
    - chiavi tecniche: validazione e pulizia
    Aggiorna e restituisce il dizionario delle tabelle.
    """
    for chiave in lista_chiavi_tecniche:
        logger.info(f"\n--- [controlla_chiavi_tecniche] Analisi chiave tecnica: {chiave} ---")

        # Esistenza colonna
        if chiave not in df.columns:
            logger.error(f"La colonna tecnica '{chiave}' NON esiste nella tabella {tabella}.")
            continue
        else:
            logger.ok(f"La colonna tecnica '{chiave}' esiste.")

        # Conversione chiave tecnica a stringa
        df[chiave] = df[chiave].astype(str)
        logger.ok(f"La chiave tecnica '{chiave}' è stata convertita a stringa.")

        serie = df[chiave]

        # NaN
        num_nan = serie.isna().sum()
        if num_nan > 0:
            logger.warning(f"La chiave tecnica '{chiave}' contiene {num_nan} valori NaN.")
        else:
            logger.ok(f"Nessun NaN nella chiave tecnica '{chiave}'.")

        # Tipi coerenti
        tipi_presenti = serie.dropna().map(type).unique()
        if len(tipi_presenti) == 1:
            logger.ok(f"Tipo coerente: {tipi_presenti[0].__name__}")
        else:
            logger.warning(f"Tipi NON coerenti: {tipi_presenti}")

    return df

def controlla_chiavi(dizionario_tabelle, relazioni, chiavi_pk, chiavi_tecniche):
    """
    Esegue i controlli di integrità sulle chiavi del modello:
    - chiavi primarie: esistenza, NaN, duplicati, tipi coerenti
    - chiavi esterne: esistenza, NaN, duplicati, orfani, tipi coerenti
    - chiavi tecniche: validazione e pulizia
    Aggiorna e restituisce il dizionario delle tabelle.
    """
    # --- PK ---
    logger.info(f"\n\n=== [controlla_chiavi] FASE 3.1: CONTROLLO CHIAVI PRIMARIE ===")
    for tabella, lista_pk in chiavi_pk.items():
        df = dizionario_tabelle[tabella]
        controlla_pk(tabella, df, lista_pk)
        df = controlla_pk(tabella, df, lista_pk)   # <-- aggiorna df
        dizionario_tabelle[tabella] = df           # <-- salva nel dizionario

    # --- FK ---
    logger.info(f"\n\n=== [controlla_chiavi] FASE 3.2: CONTROLLO CHIAVI ESTERNE ===")
    for tabella_figlia, mapping in relazioni.items():
        df_figlia = dizionario_tabelle[tabella_figlia]

        for fk, tabella_padre in mapping.items():
            df_padre = dizionario_tabelle[tabella_padre]
            pk_padre = chiavi_pk[tabella_padre][0]
            df_figlia = controlla_fk(tabella_figlia, df_figlia, fk, tabella_padre, df_padre, pk_padre)
            dizionario_tabelle[tabella_figlia] = df_figlia

    # --- CHIAVI TECNICHE ---
    logger.info(f"\n\n=== [controlla_chiavi] FASE 3.3: CONTROLLO CHIAVI TECNICHE ===")
    for tabella, lista_chiavi in chiavi_tecniche.items():
        df = dizionario_tabelle[tabella]
        df = controlla_chiavi_tecniche(tabella, df, lista_chiavi)
        dizionario_tabelle[tabella] = df

    return dizionario_tabelle

# ============================
# --- NORMALIZZAZIONE DATE ---
# ============================

def analizza_colonne_data(dizionario_tabelle):
    """
    Analizza tutte le colonne che contengono 'DATE' nel nome
    per ogni tabella del dizionario.
    Stampa:
    - numero di valori a 8 cifre
    - numero di valori NON a 8 cifre
    - eventuali valori con anno > 2262
    Restituisce una lista delle colonne che contengono 'DATE'.
    """
    dizionario_colonne_date = {}
    lista_colonne_date_globali = []

    for nome_tabella, df in dizionario_tabelle.items():
        logger.info(f"\n=== [analizza_colonne_data] Analisi colonne 'DATE' per tabella: {nome_tabella} ===")
        
        #Trova tutte le colonne che contengono "DATE"
        colonne_date = []
        for col in df.columns:
            if "DATE" in col.upper():
                colonne_date.append(col)
        dizionario_colonne_date[nome_tabella] = colonne_date

        # Aggiungi alla lista globale
        lista_colonne_date_globali.extend(colonne_date)
            
        #Se non ci sono colonne DATE, termina
        if not colonne_date:
            logger.info(f"Nessuna colonna contenente 'DATE' trovata.")
            continue
    
        # Analisi dettagliata per ogni colonna DATE
        for col in colonne_date:
            serie = df[col].astype(str).str.strip()

            # Valori a 8 cifre
            mask_8 = serie.str.match(r"^\d{8}$")
            valori_8 = serie[mask_8]

            # Valori non a 8 cifre
            mask_non_8 = ~mask_8
            valori_non_8 = serie[mask_non_8]

            logger.info(f"\n[COLONNA] {col}")
            logger.info(f" - Valori a 8 cifre: {len(valori_8)}")
            logger.info(f" - Valori NON a 8 cifre: {len(valori_non_8)}")

            # Controllo anno fuori range pandas
            fuori_range = []
            for x in valori_8:
                anno = int(x[:4])
                if anno > 2262:
                    fuori_range.append(x)

            if fuori_range:
                logger.warning(f"{len(fuori_range)} valori con anno > 2262 --> fuori range pandas")
                logger.info(f"Esempi: {sorted(set(fuori_range))[:5]}")

    return lista_colonne_date_globali, dizionario_colonne_date

def normalizza_colonne_data(dizionario_tabelle, tabelle_colonne_data):
    """
    Normalizza le colonne DATE per tutte le tabelle nel dizionario_tabelle,
    usando il dizionario tabelle_colonne_data: { nome_tabella : [colonne_date] }
    Restituisce un nuovo dizionario_tabelle aggiornato.
    """

    if not tabelle_colonne_data:
        logger.info(f"Nessuna informazione sulle colonne 'DATE' da normalizzare.")
        return dizionario_tabelle

    for nome_tabella, df in dizionario_tabelle.items():
        colonne_date = tabelle_colonne_data.get(nome_tabella, [])

        if not colonne_date:
            continue

        # Filtra solo le colonne effettivamente presenti nel df
        colonne_presenti = [c for c in colonne_date if c in df.columns]

        if not colonne_presenti:
            logger.info(f"\nNessuna delle colonne 'DATE' è presente nel DataFrame per '{nome_tabella}'.")
            continue

        logger.info(f"\n=== [normalizza_colonne_data] Normalizzazione colonne DATE per tabella: {nome_tabella} ===")
        logger.info(f"Colonne DATE da normalizzare: {colonne_presenti}")

        for col in colonne_presenti:
            serie = df[col].astype(str).str.strip()
            nuova_colonna = []

            for x in serie:
                # Caso: formato YYYYMMDD
                if len(x) == 8 and x.isdigit():
                    anno = int(x[:4])
                    mese = int(x[4:6])
                    giorno = int(x[6:8])

                    # Caso speciale: anno fuori range pandas
                    if anno > 2262:
                        nuova_colonna.append(f"{anno:04d}-{mese:02d}-{giorno:02d}")
                        continue

                    # Conversione normale
                    try:
                        dt = pd.to_datetime(x, format="%Y%m%d", errors="coerce")
                        nuova_colonna.append(dt.strftime("%Y-%m-%d") if not pd.isna(dt) else None)
                    except:
                        nuova_colonna.append(None)

                else:
                    # Fallback per formati non a 8 cifre
                    try:
                        dt = pd.to_datetime(x, errors="coerce")
                        nuova_colonna.append(dt.strftime("%Y-%m-%d") if not pd.isna(dt) else None)
                    except:
                        nuova_colonna.append(None)

            df[col] = pd.to_datetime(df[col], errors="coerce")

            num_nat = df[col].isna().sum()
            logger.ok(f"Conversione completata per '{col}' in '{nome_tabella}'.")
            logger.info(f"Valori NaT finali: {num_nat}")

        # aggiorna la tabella nel dizionario
        dizionario_tabelle[nome_tabella] = df

    return dizionario_tabelle

# =========================
# GESTIONE NaN (pre merge)
# =========================

def gestisci_NaN_pre_merge(tabelle_dict, tabelle_colonne_data, colonne_numeriche_critiche=None):
    """
    Gestisce i NaN PRIMA del merge, su tutte le tabelle del dizionario.
    Regole:
    - Colonne DATE → ERRORE + eliminazione righe
    - Colonne numeriche critiche → ERRORE + eliminazione righe
    - Colonne stringa → WARNING, nessuna eliminazione
    - Colonne numeriche non critiche → WARNING, nessuna eliminazione
    """
    if colonne_numeriche_critiche is None:
        colonne_numeriche_critiche = []

    for nome_tab, df in tabelle_dict.items():
        logger.info(f"\n--- [gestisci_NaN_pre_merge] Analisi NaN per tabella: {nome_tab} ---")

        # Colonne DATE specifiche per questa tabella
        colonne_date_tabella = tabelle_colonne_data.get(nome_tab, [])

        nan_trovati = False  # <--- flag per capire se la tabella ha NaN

        for col in df.columns:
            num_nan = df[col].isna().sum()
            if num_nan == 0:
                continue

            nan_trovati = True  # <--- segna che almeno un NaN è stato trovato

            # --- 1) Colonne DATE ---
            if col in colonne_date_tabella:
                logger.error(f"La colonna DATA '{col}' contiene {num_nan} NaN.")
                logger.info(f"Eliminazione delle righe con NaN in '{col}'.")
                df = df.dropna(subset=[col])
                tabelle_dict[nome_tab] = df
                continue

            # --- 2) Colonne numeriche critiche ---
            if col in colonne_numeriche_critiche:
                logger.error(f"La colonna numerica critica '{col}' contiene {num_nan} NaN.")
                logger.info(f"Eliminazione delle righe con NaN in '{col}'.")
                df = df.dropna(subset=[col])
                tabelle_dict[nome_tab] = df
                continue

            # --- 3) Colonne stringa ---
            if df[col].dtype == object:
                logger.warning(f"La colonna stringa '{col}' contiene {num_nan} NaN.")
                continue

            # --- 4) Colonne numeriche NON critiche ---
            if pd.api.types.is_numeric_dtype(df[col]):
                logger.warning(f"La colonna numerica '{col}' contiene {num_nan} NaN.")
                continue

            # --- 5) Fallback ---
            logger.warning(f"La colonna '{col}' contiene {num_nan} NaN. Nessuna azione necessaria")

      # Se nessun NaN è stato trovato nella tabella
        if not nan_trovati:
            logger.ok(f"Nessun valore NaN trovato.")      

    return tabelle_dict

# ============================
# --- ANALISI MONOVARIATA ---
# ============================

def analisi_monovariata(tabelle_dict):
    """
    Esegue analisi monovariata per tutte le tabelle nel dizionario:
    - Statistiche numeriche (describe)
    - Statistiche categoriche (value_counts)
    - Range date
    Stampa tutto nel file di log.
    """
    for nome_tab, df in tabelle_dict.items():
        logger.info(f"\n\n--- [analisi_monovariata] ANALISI MONOVARIATA: {nome_tab} ---")

        # 1. ANALISI NUMERICA
        numeriche = df.select_dtypes(include=["number"])
        if not numeriche.empty:
            logger.info(f"[{nome_tab}] STATISTICHE NUMERICHE:")
            logger.info("\n" + numeriche.describe().to_string())
        else:
            logger.info(f"[{nome_tab}] Nessuna colonna numerica trovata.")

        # 2. ANALISI CATEGORICA
        categoriche = df.select_dtypes(include=["object", "category", "string", "str"])
        if not categoriche.empty:
            logger.info(f"[{nome_tab}] STATISTICHE CATEGORICHE (top 5 valori):")
            for col in categoriche.columns:
                vc = df[col].value_counts(dropna=False).head(5)
                logger.info(f"\nColonna: {col}\n{vc.to_string()}")
        else:
            logger.info(f"[{nome_tab}] Nessuna colonna categorica trovata.")

        # 3. ANALISI DATE
        colonne_date = [c for c in df.columns if "DATE" in c.upper()]
        if colonne_date:
            logger.info(f"[{nome_tab}] ANALISI COLONNE 'DATE':")
            for col in colonne_date:
                try:
                    logger.info(
                        f"  - {col}: min={df[col].min()}, max={df[col].max()}"
                    )
                except:
                    logger.warning(f"Impossibile calcolare range per {col}.")
        else:
            logger.info(f"[{nome_tab}] Nessuna colonna data trovata.")

# =====================
# --- CORRELAZIONI ---
# =====================

def analisi_correlazioni(tabelle_dict):
    """
    Calcola la matrice di correlazione per tutte le tabelle nel dizionario.
    - Considera solo colonne numeriche
    - Logga la matrice nel file di log
    - Salta le tabelle senza almeno 2 colonne numeriche
    """
    for nome_tab, df in tabelle_dict.items():
        # Seleziona solo colonne numeriche
        numeriche = df.select_dtypes(include=["number"])

        # Se meno di 2 colonne numeriche → nessuna correlazione, nessun log
        if numeriche.shape[1] < 2:
            continue

        # Se ci sono almeno 2 colonne numeriche → calcola e logga
        logger.info(f"\n--- [analisi_correlazioni] ANALISI CORRELAZIONI: {nome_tab} ---")

        try:
            corr = numeriche.corr().round(3)
            logger.info(f"[{nome_tab}] Matrice di correlazione:")
            logger.info("\n" + corr.to_string())
        except Exception as e:
            logger.error(f"[{nome_tab}] Errore nel calcolo della correlazione: {e}")

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
        logger.error(f"[calcola_margini] Impossibile calcolare VAL_MARGIN nella tabella {nome_tabella}: "
                     f"colonne mancanti: {mancanti}")
        return df

    # Calcolo margini con gestione errori
    try:
        df["VAL_MARGIN"] = df["VAL_REVENUES"] - df["VAL_COST"]
        logger.ok(f"[calcola_margini] Colonna VAL_MARGIN calcolata correttamente nella tabella {nome_tabella}.")
    except Exception as e:
        logger.error(f"[calcola_margini] Errore durante il calcolo di VAL_MARGIN nella tabella {nome_tabella}: {e}")

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
            logger.error(f"La colonna FK '{fk}' non esiste nella tabella {tabella_figlia}. Merge impossibile.")
            continue

        df_padre = dizionario_tabelle[tabella_padre]

        if fk not in df_padre.columns:
            logger.error(f"La colonna PK '{fk}' non esiste nella tabella padre {tabella_padre}. Merge impossibile.")
            continue

        # Righe prima del merge
        righe_prima = len(df)

        logger.info(f"Avvio merge LEFT JOIN tra {tabella_figlia} e {tabella_padre} sulla chiave '{fk}'.")
        logger.info(f"Righe tabella figlia: {len(df)} | Righe tabella padre: {len(df_padre)}")

        try:
            df = df.merge(
                df_padre,
                how="left",
                left_on=fk,
                right_on=fk,
                suffixes=("", f"_{tabella_padre}")
            )
        except Exception as e:
            logger.error(f"Merge fallito tra {tabella_figlia} e {tabella_padre}: {e}")
            continue

        # Righe dopo il merge
        righe_dopo = len(df)

        # Log esito
        if righe_dopo == righe_prima:
            logger.ok(f"Merge completato senza variazioni nel numero di righe ({righe_dopo}).")
        elif righe_dopo > righe_prima:
            logger.warning(f"Il merge ha aumentato le righe: {righe_prima} → {righe_dopo}.")
            logger.warning(f"Possibile duplicazione dovuta a chiavi non univoche nella tabella padre.")
        else:
            logger.warning(f"Il merge ha RIDOTTO le righe: {righe_prima} → {righe_dopo}.")
            logger.warning(f"Questo NON dovrebbe accadere con un LEFT JOIN. Verificare i dati.")

        # Ricorsione
        df = merge_tabelle(df, tabella_padre, relazioni, dizionario_tabelle)

    return df

# ==========================
# TOVARE I NaN (post-merge)
# ==========================

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
            logger.warning(f"La colonna {col} contiene: {num_nan} NaN")

    if not colonne_nan:
        logger.ok(f"Nessuna colonna contiene NaN.")

    return colonne_nan
    
# ==========================
# GESTIONE NaN (post merge)
# ==========================

def gestisci_NaN_post_merge(df, colonne_date, colonne_numeriche_critiche=None, colonne_derivate=None):
    """
    Gestisce i NaN secondo le regole definite:
    - Colonne stringa non chiave: WARNING, ignorare
    - Colonne data: ERRORE, eliminazione righe
    - Colonne numeriche critiche: ERRORE, eliminazione righe
    - Colonne derivate: ERRORE, non ammessi
    """

    if colonne_numeriche_critiche is None:
        colonne_numeriche_critiche = []

    if colonne_derivate is None:
        colonne_derivate = []

    # Loop su tutte le colonne
    for col in df.columns:
        num_nan = df[col].isna().sum()
        if num_nan == 0:
            continue

        # --- 1) Colonne derivate ---
        if col in colonne_derivate:
            logger.error(f"La colonna derivata '{col}' contiene {num_nan} NaN. "
                         f"I valori derivati NON devono avere NaN. Interrompere l'ETL.")
            continue

        # --- 2) Colonne data ---
        if col in colonne_date:
            logger.error(f"La colonna DATA '{col}' contiene {num_nan} NaN.")
            logger.info(f"Eliminazione delle righe con NaN in '{col}'.")
            df = df.dropna(subset=[col])
            continue

        # --- 3) Colonne numeriche critiche ---
        if col in colonne_numeriche_critiche:
            logger.error(f"La colonna numerica critica '{col}' contiene {num_nan} NaN.")
            logger.info(f"Eliminazione delle righe con NaN in '{col}'.")
            df = df.dropna(subset=[col])
            continue

        # --- 4) Colonne stringa non chiave ---
        if df[col].dtype == object:
            logger.warning(f"La colonna stringa '{col}' contiene {num_nan} NaN.")
            logger.info(f"Nessuna eliminazione: i NaN vengono ignorati.")
            continue

        # --- 5) Colonne numeriche NON critiche ---
        if pd.api.types.is_numeric_dtype(df[col]):
            logger.warning(f"La colonna numerica '{col}' contiene {num_nan} NaN.")
            logger.info(f"Nessuna eliminazione (colonna non critica).")
            continue

        # --- 6) Fallback ---
        logger.warning(f"La colonna '{col}' contiene {num_nan} NaN ma verranno ignorati.")
        logger.info(f"essuna eliminazione.")

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
        logger.error(f"La colonna 'ID_ORDER_DATE' non esiste nel DataFrame.")
        return df

    #Controllo tipo
    if not pd.api.types.is_datetime64_any_dtype(df["ID_ORDER_DATE"]):
        logger.info(f"'ID_ORDER_DATE' non è datetime. Conversione in corso...")
        try:
            df["ID_ORDER_DATE"] = pd.to_datetime(df["ID_ORDER_DATE"], errors="coerce")
            logger.ok(f"Conversione completata.")
        except Exception as e:
            logger.error(f"Conversione fallita: {e}")
            return df

    #Creazione colonne derivate
    df["ORDER_YEAR"] = df["ID_ORDER_DATE"].dt.year
    df["ORDER_MONTH"] = df["ID_ORDER_DATE"].dt.month
    df["ORDER_WEEK"] = df["ID_ORDER_DATE"].dt.isocalendar().week

    logger.ok(f"Colonne ORDER_YEAR, ORDER_MONTH e ORDER_WEEK create correttamente.")

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
    logger.ok(f"File OLAP salvato correttamente: {nome_file}")
    logger.info(f"Dimensioni: {righe} righe, {colonne} colonne")

# ============
# --- MAIN ---
# ============

def main(carica=True, salva=True, tabelle=None):
    # Caricamento dei file .csv
    logger.info("\n\n=== [main] FASE 1: CARICAMENTO DATASET ===")
    
    # Preparazione tabelle
    tabelle = prepara_tabelle(carica, tabelle)
    if tabelle is None:
        return None

    # Verifica tabelle attese
    if not verifica_tabelle_attese(tabelle):
        return None

    # Eliminazione delle righe duplicate dalle tabelle
    logger.info("\n\n=== [main] FASE 2: CONTROLLO DUPLICATI ===")
    tabelle = rimuovi_duplicati(tabelle)

    # Controllo delle chiavi primarie, esterne e tecniche
    logger.info("\n\n=== [main] FASE 3: CONTROLLO CHIAVI ===")
    relazioni, chiavi_pk, chiavi_tecniche = configura_chiavi()
    tabelle = controlla_chiavi(tabelle, relazioni, chiavi_pk, chiavi_tecniche)

    # Normalizzazione colonne DATE
    logger.info("\n\n==== [main] FASE 4.1: ANALISI COLONNE DATA ====")
    colonne_data, tabelle_colonne_data = analizza_colonne_data(tabelle)
    logger.info("\n\n==== [main] FASE 4.2: NORMALIZZAZIONE COLONNE DATA ====")
    tabelle = normalizza_colonne_data(tabelle, tabelle_colonne_data)

    # Gestione NaN pre-merge
    logger.info("\n\n=== [main] FASE 5: GESTIONE NAN PRE-MERGE ===")
    tabelle = gestisci_NaN_pre_merge(
        tabelle,
        tabelle_colonne_data,
        colonne_numeriche_critiche=["VAL_REVENUES", "VAL_COST"])

    # Analisi Monovariata
    logger.info("\n\n=== [main] FASE 6: ANALISI MONOVARIATA ===")
    analisi_monovariata(tabelle)

    # Correlazioni
    logger.info("\n\n=== [main] FASE 7: CORRELAZIONI ===")
    analisi_correlazioni(tabelle)
    
    # Arricchimento dati: aggiunta della colonna MARGINI
    logger.info("\n\n=== [main] FASE 8: CALCOLO COLONNE DERIVATE: MARGINI ===")
    df_sales = tabelle["SALES"]
    df_sales = calcola_margini(df_sales, nome_tabella="SALES")
    tabelle["SALES"] = df_sales

    # Merge
    logger.info("\n\n=== [main] FASE 9: MERGE TABELLE ===")
    df_merge = tabelle["SALES"].copy()
    df_merge = merge_tabelle(df_merge, "SALES", relazioni, tabelle)

    # Controllo NaN post-merge
    logger.info("\n\n=== [main] FASE 10: CONTROLLO NAN POST-MERGE ===")
    colonne_con_nan = trovare_nan(df_merge)

    # Gestione NaN post-merge
    if colonne_con_nan:
        logger.info(f"\n[main] Esecuzione di gestisci_NaN_post_merge.")
        logger.info("\n\n=== [main] FASE 10.bis: GESTIONE NAN POST-MERGE ===")
        df_merge = gestisci_NaN_post_merge(
            df_merge,
            colonne_data,
            colonne_numeriche_critiche=["VAL_REVENUES", "VAL_COST"],
            colonne_derivate=["VAL_MARGIN"])
    
    # Aggiunta colonne ORDER_YEAR, ORDER_MESE, ORDER_WEEK
    logger.info("\n\n=== [main] FASE 11: CREAZIONE ATTRIBUTI TEMPORALI ===")
    df_merge = aggiungi_order_features(df_merge)
    
    # Salvataggio del file OLAP finale SOLO se salva=True
    if salva:
        logger.info("\n\n=== [main] FASE 12: DOWNLOAD DEL FILE OLAP ===")
        salva_dataset_olap(df_merge, nome_file="olap.csv")

    return df_merge

if __name__ == "__main__":
    main()