import pandas as pd

def carica_csv(nome_file):
    """
    Carica un CSV presente nella stessa cartella del notebook.
    Se il file è vuoto o non leggibile, restituisce None.
    """
    print(f"[INFO] Caricamento file: {nome_file}")

    try:
        #Lettura del file CSV
        df = pd.read_csv(nome_file)
        righe, colonne = df.shape
        #Stampa informazioni sulla tabella caricata
        print(f"[OK] {nome_file} caricato ({righe} righe, {colonne} colonne)")

        #Controllo tabella vuota
        if righe == 0:
            print(f"[ERRORE] La tabella '{nome_file}' è vuota. Verrà esclusa dal processo.")
            return None
        #Restituisce un DatFrame valido
        return df

    except Exception as e:
        #Gestione errori di lettura (file mancante, formati errati, permessi, ...)
        print(f"[ERRORE] Impossibile leggere {nome_file}: {e}")
        return None

def carica_tabelle():
    #Dizionario dei file da caricare
    file_paths = {
                  "SALES": "SALES.csv",
                  "AREA_MANAGER_LOOKUP": "AREA_MANAGER_LOOKUP.csv",
                  "COMPANY_LOOKUP": "COMPANY_LOOKUP.csv",
                  "CUSTOMER_LOOKUP": "CUSTOMER_LOOKUP.csv",
                  "ITEM_BUSINESS_LINE_LOOKUP": "ITEM_BUSINESS_LINE_LOOKUP.csv",
                  "ITEM_LOOKUP": "ITEM_LOOKUP.csv"
                 }
    tabelle = {}
    for nome, path in file_paths.items():
        tabelle[nome] = carica_csv(path)

    return tabelle

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
    print(f"\n=== Controllo PK per tabella: {tabella} ===")

    for pk in lista_pk:
        print(f"\n[INFO] Analisi PK: {pk}")

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

def controlla_chiavi(dizionario_tabelle):
    #Relazioni esplicite (FK → tabella padre)
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
    #Chiavi primarie
    chiavi_pk = {
        "SALES": [],
        "CUSTOMER_LOOKUP": ["IDS_CUSTOMER"],
        "ITEM_LOOKUP": ["IDS_ITEM"],
        "AREA_MANAGER_LOOKUP": ["ID_AREA_MANAGER"],
        "ITEM_BUSINESS_LINE_LOOKUP": ["ID_BUSINESS_LINE"],
        "COMPANY_LOOKUP" : ["ID_COMPANY"]
    }
    #Chiavi tecniche (da ignorare)
    chiavi_tecniche = {
        "SALES": ["ID_ORDER_NUM", "ID_ORDER_DATE", "ID_INVOICE_DATE"],
        "CUSTOMER_LOOKUP": ["ID_COUNTRY"]
    }
    # --- PK ---
    for tabella, lista_pk in chiavi_pk.items():
        df = dizionario_tabelle[tabella]
        controlla_pk(tabella, df, lista_pk)

    # --- FK ---
    for tabella_figlia, mapping in relazioni.items():
        df_figlia = dizionario_tabelle[tabella_figlia]

        for fk, tabella_padre in mapping.items():
            df_padre = dizionario_tabelle[tabella_padre]
            pk_padre = chiavi_pk[tabella_padre][0]  # assumiamo PK singola
            controlla_fk(tabella_figlia, df_figlia, fk, tabella_padre, df_padre, pk_padre)

def merge_modello(relazioni, dizionario_tabelle, tabella_base="SALES"):
    """
    Esegue il merge completo del modello seguendo le relazioni esplicite.
    - Parte da SALES
    - Mergia tutte le lookup di primo livello
    - Mergia anche le lookup collegate alle lookup (ricorsivo)
    """

    df_finale = dizionario_tabelle[tabella_base].copy()

    def merge_lookup(df, tabella_figlia):
        """Merge ricorsivo delle lookup collegate a una tabella."""
        if tabella_figlia not in relazioni:
            return df  # Nessuna relazione da espandere

        for fk, tabella_padre in relazioni[tabella_figlia].items():
            df_padre = dizionario_tabelle[tabella_padre]

            # Merge LEFT JOIN
            df = df.merge(
                df_padre,
                how="left",
                left_on=fk,
                right_on=fk,
                suffixes=("", f"_{tabella_padre}")
            )

            # Merge ricorsivo per le lookup collegate alla tabella padre
            df = merge_lookup(df, tabella_padre)

        return df

    # Avvia il merge ricorsivo partendo da SALES
    df_finale = merge_lookup(df_finale, tabella_base)

    return df_finale

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

def salva_file_olap(df, nome_file="sales_merge.csv"):
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

def main(salva=True):
    #Caricamento dei file csv
    print("\n=== CARICAMENTO TABELLE ===")
    tabelle = carica_tabelle()

    #Eliminazione delle righe duplicate dalle tabelle
    print("\n=== RIMOZIONE DUPLICATI ===")
    tabelle = rimuovi_duplicati(tabelle)
    
    print("\n=== CONTROLLO CHIAVI ===")
    relazioni, chiavi_pk, chiavi_tecniche = configura_chiavi()
    controlla_chiavi(tabelle)
    
    #Aggiunta colonna MARGINI
    df_sales = tabelle["SALES"]
    df_sales["VAL_MARGIN"] = df_sales["VAL_REVENUES"] - df_sales["VAL_COST"]
    tabelle["SALES"] = df_sales

    #Merge
    df_merge = merge_modello(relazioni, tabelle, tabella_base="SALES")

    #Data cleaning: NaN
    print("\n=== CONTROLLO NAN ===")
    colonne_con_nan = trovare_nan(df_merge)
    
    #Normalizzazione colonne DATE
    print("\n===== ANALISI COLONNE DATA =====")
    colonne_data = analizza_colonne_date(df_merge)
    print("\n===== NORMALIZZAZIONE COLONNE DATA =====")
    df_merge = normalizza_colonne_date(df_merge, colonne_data)
    
    #Aggiunta colonne
    print("\n===== AGGIUNTA DELLE COLONNE ORDER_YEAR, ORDER_MESE, ORDER_WEEK =====")
    df_merge = aggiungi_order_features(df_merge)
    
    #Scaricare il file OLAP finale
    print("\n=== DOWNLOAD DEL FILE OLAP ===")
    salva_file_olap(df_merge)

    #Scaricare il file OLAP finale SOLO se salva=True
    if salva:
        print("\n=== DOWNLOAD DEL FILE OLAP ===")
        salva_file_olap(df_merge)

    return df_merge

if __name__ == "__main__":
    main()