[README.md](https://github.com/user-attachments/files/26655898/README.md)
# Analisi e Previsione delle Vendite

Un'applicazione Python standalone che analizza dati storici di vendita, genera report statistici, addestra modelli di previsione ed esporta un dataset OLAP denormalizzato pronto per Power BI.

**Autori:** Giuseppe Budano · Giuseppe di Gesu · Elena Demaria · Francesco Falorni  
**Licenza:** Tutti i diritti riservati — progetto privato

---

## Indice

1. [Cosa fa](#cosa-fa)
2. [Struttura del progetto](#struttura-del-progetto)
3. [Requisiti](#requisiti)
4. [Installazione](#installazione)
5. [Utilizzo](#utilizzo)
   - [Opzione A — Streamlit](#opzione-a----avvio-tramite-streamlit-consigliata)
   - [Opzione B — Import diretto](#opzione-b----importazione-diretta-dei-moduli)
6. [Formato dei dati di input](#formato-dei-dati-di-input)
7. [Output generati](#output-generati)
8. [Modello di previsione](#modello-di-previsione)
9. [Limitazioni note](#limitazioni-note)

---

## Cosa fa

L'applicazione esegue una pipeline completa end-to-end articolata in 12 fasi:

1. **Caricamento** — carica i 6 file CSV da disco o direttamente dalla memoria (quando usata via Streamlit), con gestione degli errori e logging su file.
2. **Deduplicazione** — rimuove le righe duplicate da ciascuna tabella, loggando il numero di righe eliminate.
3. **Validazione chiavi** — controlla la consistenza di chiavi primarie, esterne e tecniche tra tutte le tabelle.
4. **Normalizzazione date** — rileva automaticamente le colonne data in formato intero `YYYYMMDD` e le converte in `datetime`; segnala e gestisce anomalie (es. anno 2788 nei dati di test).
5. **Gestione NaN pre-merge** — applica regole differenziate per colonne data, numeriche critiche e descrittive, con eliminazione selettiva delle righe.
6. **Analisi monovariata** — esegue un'analisi qualitativa e quantitativa su ogni tabella prima del merge.
7. **Analisi correlazioni** — calcola le correlazioni tra variabili numeriche sulle singole tabelle sorgente.
8. **Calcolo margini** — aggiunge la colonna derivata `VAL_MARGIN = VAL_REVENUES - VAL_COST` alla tabella SALES.
9. **Merge OLAP** — unisce le 6 tabelle in un'unica tabella denormalizzata tramite LEFT JOIN ricorsivo.
10. **Gestione NaN post-merge** — rileva e gestisce i NaN introdotti dal merge secondo le stesse regole differenziate.
11. **Attributi temporali** — aggiunge le colonne `ORDER_YEAR`, `ORDER_MONTH` e `ORDER_WEEK` derivate dalla data ordine.
12. **Esportazione** — salva il dataset OLAP in CSV e i risultati della previsione per l'utilizzo in Power BI.

---

## Struttura del progetto

```
project/
│
├── main.py                         # Entry point Streamlit — orchestra l'intera pipeline
│
├── modules/
│   ├── etl.py                      # ETL: caricamento, pulizia, validazione, merge, esportazione
│   ├── stat_mono.py                # Analisi statistica: qualitativa, numerica, categorica
│   ├── stat_xlsx.py                # Esportazione statistiche in formato Excel multi-foglio
│   ├── grafici.py                  # Visualizzazione dati: grafici e plot
│   └── ForecastingLib.py           # Forecasting: aggregazione, training, previsioni, grafici
│
├── images/
│   └── logo_cefla.png              # Logo visualizzato nell'intestazione Streamlit
│
├── SALES.csv                       # Tabella dei fatti
├── CUSTOMER_LOOKUP.csv             # Dimensione clienti
├── ITEM_LOOKUP.csv                 # Dimensione articoli/prodotti
├── COMPANY_LOOKUP.csv              # Dimensione aziende
├── AREA_MANAGER_LOOKUP.csv         # Dimensione area manager
├── ITEM_BUSINESS_LINE_LOOKUP.csv   # Dimensione business line
│
├── output/                         # File di output generati automaticamente
│   ├── olap.csv                    # Tabella OLAP denormalizzata
│   ├── Sales_Forecast_PowerBI.csv  # Storico + previsioni per Power BI
│   ├── report_statistiche.xlsx     # Report statistiche multi-foglio
│   └── *.png                       # Grafici di forecast esportati
│
├── logs/                           # Log ETL con timestamp (generati automaticamente)
│   └── etl_YYYYMMDD_HHMMSS.log
│
└── README.md
```

---

## Requisiti

È richiesto Python 3.9 o superiore.

Installa tutte le dipendenze con:

```bash
pip install -r requirements.txt
```

**requirements.txt**

```
pandas
numpy
matplotlib
seaborn
scikit-learn
openpyxl
streamlit
```

---

## Installazione

```bash
# 1. Clona la repository
git clone https://github.com/your-org/sales-analysis-forecasting.git
cd sales-analysis-forecasting

# 2. (Opzionale) Crea un ambiente virtuale
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows

# 3. Installa le dipendenze
pip install -r requirements.txt
```

---

## Utilizzo

### Opzione A — Avvio tramite Streamlit (consigliata)

```bash
streamlit run main.py
```

L'interfaccia web si aprirà automaticamente nel browser. Da lì è possibile:

1. Caricare i 6 file CSV tramite il pannello di upload
2. Cliccare su **"Avvia Pipeline Completa"** per eseguire l'intera pipeline ETL → Statistiche → Grafici → Previsioni
3. Visualizzare le metriche di valutazione dei modelli (MAE, RMSE, R²) con dettaglio per fold
4. Scaricare il report statistiche in Excel, il dataset OLAP e le previsioni per Power BI

I dati elaborati vengono mantenuti in `st.session_state` per evitare ricalcoli ad ogni interazione con l'interfaccia.

### Opzione B — Importazione diretta dei moduli

Ogni modulo può essere utilizzato in modo autonomo:

```python
import pandas as pd
from modules import etl
from modules.stat_mono import analisi_qualitativa, statistiche_numeriche, statistiche_categoriche
from modules.stat_xlsx import export_statistiche_excel
from modules.grafici import imposta_stile, plot_trend_ricavi_tempo, plot_rfm
from modules import ForecastingLib

# Esegui l'ETL completo e ottieni il DataFrame OLAP
olap = etl.main(carica=True, salva=True)

# Esegui l'analisi statistica
analisi_qualitativa(olap, "OLAP")
statistiche_numeriche(olap, "OLAP")
statistiche_categoriche(olap, "OLAP")

# Esporta le statistiche in Excel
export_statistiche_excel({"SALES_OLAP": olap}, nome_file="report_statistiche.xlsx")

# Genera i grafici
imposta_stile()
plot_trend_ricavi_tempo(olap)
plot_rfm(olap)

# Esegui il forecasting con entrambi i modelli
aggregazioni = ForecastingLib.aggrega_dati(olap)

modello_lr,  metriche_lr  = ForecastingLib.addestra_modello(aggregazioni["mensile"])
modello_gbr, metriche_gbr = ForecastingLib.addestra_modello_gbr(aggregazioni["mensile"])

previsioni_lr  = ForecastingLib.genera_previsioni(modello_lr,  aggregazioni["mensile"], n_mesi=6)
previsioni_gbr = ForecastingLib.genera_previsioni(modello_gbr, aggregazioni["mensile"], n_mesi=6)

# Grafici di confronto
ForecastingLib.grafico_confronto_modelli(aggregazioni["mensile"], previsioni_lr, previsioni_gbr)
ForecastingLib.grafico_confronto_orizzonti(
    aggregazioni["mensile"],
    {
        "6 mesi":  previsioni_lr,
        "12 mesi": ForecastingLib.genera_previsioni(modello_lr, aggregazioni["mensile"], n_mesi=12)
    }
)
```

---

## Formato dei dati di input

Tutti e 6 i file CSV devono trovarsi nella stessa cartella degli script (o essere caricati tramite l'interfaccia Streamlit). Le colonne attese sono:

| File | Colonne principali |
|------|-------------------|
| `SALES.csv` | `ID_COMPANY`, `ID_ORDER_NUM`, `IDS_CUSTOMER`, `IDS_ITEM`, `ID_ORDER_DATE`, `ID_INVOICE_DATE`, `VAL_REVENUES`, `VAL_COST` |
| `CUSTOMER_LOOKUP.csv` | `IDS_CUSTOMER`, `DESC_CUSTOMER`, `ID_COUNTRY`, `ID_AREA_MANAGER` |
| `ITEM_LOOKUP.csv` | `IDS_ITEM`, `DESC_ITEM`, `ID_BUSINESS_LINE` |
| `COMPANY_LOOKUP.csv` | `ID_COMPANY`, `DESC_COMPANY` |
| `AREA_MANAGER_LOOKUP.csv` | `ID_AREA_MANAGER`, `DESC_AREA_MANAGER` |
| `ITEM_BUSINESS_LINE_LOOKUP.csv` | `ID_BUSINESS_LINE`, `DESC_BUSINESS_LINE` |

> **Nota:** le colonne data (`ID_ORDER_DATE`, `ID_INVOICE_DATE`) sono attese in formato intero `YYYYMMDD`. L'ETL gestisce la conversione automatica e segnala eventuali anomalie nel log.

---

## Output generati

| File | Descrizione |
|------|-------------|
| `olap.csv` | Tabella OLAP denormalizzata — tutti e 6 i dataset uniti in un unico file piatto, pronto per Power BI |
| `Sales_Forecast_PowerBI.csv` | Storico mensile + previsioni con colonne `DATE`, `FORECAST_REVENUES` e `TIPO` |
| `report_statistiche.xlsx` | Report Excel multi-foglio: riepilogo, info generali, missing values, statistiche numeriche e categoriche per dataset |
| `forecast_singolo.png` | Storico mensile + previsioni di un singolo modello |
| `confronto_modelli.png` | Confronto visivo tra Regressione Lineare e GradientBoosting sullo stesso orizzonte |
| `confronto_orizzonti.png` | Confronto tra previsioni su orizzonti temporali diversi (es. 6 e 12 mesi) |
| `logs/etl_*.log` | Log completo dell'esecuzione ETL con timestamp e livelli INFO / OK / WARNING / ERROR |

---

## Modello di previsione

Il modulo `ForecastingLib.py` implementa due modelli predittivi, entrambi valutati con **TimeSeriesSplit a 3 fold** per garantire che la validazione avvenga sempre su dati futuri rispetto al training.

### Regressione Lineare (`addestra_modello`)

Utilizza `LinearRegression` di scikit-learn con tre feature costruite ad hoc:

- `MONTH_IDX` — indice progressivo del mese (0, 1, 2, …), cattura il trend generale
- `ORDER_MONTH` — numero del mese (1–12), cattura la stagionalità
- `LAG_1` — ricavi del mese precedente, cattura il momentum a breve termine

### Gradient Boosting Regressor (`addestra_modello_gbr`)

Utilizza `GradientBoostingRegressor` di scikit-learn con le stesse feature della regressione lineare, per confrontare le previsioni di un approccio ensemble con quelle del modello lineare. Restituisce anche l'importanza delle feature per ciascun fold.

### Strategia di previsione

Entrambi i modelli vengono riallenati su tutti i dati disponibili dopo la fase di valutazione. Le previsioni future utilizzano una **strategia autoregressiva**: ogni valore previsto diventa il `LAG_1` del passo successivo. Le metriche restituite per ciascun modello sono MAE, RMSE e R² medi sui fold, con dettaglio per singolo fold.

---

## Limitazioni note

- Il software non supporta formati di input diversi dal CSV: file Excel, JSON o database relazionali richiedono una fase di conversione preliminare prima di poter essere utilizzati.
- Non è prevista una gestione automatica degli aggiornamenti incrementali: ogni esecuzione rielabora l'intera pipeline dall'inizio, senza la possibilità di aggiornare solo i dati più recenti.
- L'interfaccia Streamlit non supporta l'autenticazione utente: l'applicazione è pensata per un utilizzo locale o in ambienti di rete protetti, e non è adatta a un deployment pubblico senza un layer di sicurezza aggiuntivo.
- Il modulo di forecasting supporta esclusivamente l'aggregazione mensile: granularità giornaliera o settimanale non sono attualmente gestite.
- I grafici di forecast vengono esportati in formato PNG nella cartella `output/`; i grafici dell'analisi esplorativa vengono renderizzati direttamente nell'interfaccia Streamlit e non salvati su disco.
