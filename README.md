[README.md](https://github.com/user-attachments/files/26647847/README.1.md)
# Analisi e Previsione delle Vendite

Un'applicazione Python standalone che analizza dati storici di vendita, genera report statistici, addestra un modello di previsione ed esporta un dataset OLAP denormalizzato pronto per Power BI.

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
   - [Opzione B — Import diretto](#opzione-c----importazione-diretta-dei-moduli)
6. [Formato dei dati di input](#formato-dei-dati-di-input)
7. [Output generati](#output-generati)
8. [Modello di previsione](#modello-di-previsione)
9. [Limitazioni note](#limitazioni-note)

---

## Cosa fa

L'applicazione esegue una pipeline completa end-to-end:

1. **ETL** — carica 6 file CSV, rimuove i duplicati, valida le chiavi primarie e straniere, normalizza le colonne data, calcola la colonna margine e unisce tutto in un'unica tabella OLAP denormalizzata.
2. **Analisi statistica** — esegue un'analisi qualitativa e quantitativa su ogni tabella: valori mancanti, cardinalità, distribuzioni numeriche (min, media, max, deviazione standard) e frequenze categoriche.
3. **Visualizzazione dati** — genera una serie di grafici (distribuzione dei ricavi, trend nel tempo, analisi RFM, correlazioni) salvati come file SVG.
4. **Forecasting** — addestra un modello di regressione lineare con cross-validazione su serie temporali e genera una previsione dei ricavi per i 6 mesi successivi.
5. **Esportazione** — salva il dataset OLAP in CSV e i risultati della previsione per l'utilizzo in Power BI.

---

## Struttura del progetto

```
project/
│
├── main.ipynb              # Notebook principale — esegue la pipeline completa
├── etl.py                  # ETL: caricamento, pulizia, validazione, merge, esportazione
├── stat_mono.py            # Analisi statistica: qualitativa, numerica, categorica
├── grafici.py              # Visualizzazione dati: grafici e plot
├── ForecastingLib.py       # Forecasting: aggregazione, training, previsioni
│
├── SALES.csv                       # Tabella delle vendite
├── CUSTOMER_LOOKUP.csv             # Dimensione clienti
├── ITEM_LOOKUP.csv                 # Dimensione articoli/prodotti
├── COMPANY_LOOKUP.csv              # Dimensione aziende
├── AREA_MANAGER_LOOKUP.csv         # Dimensione area manager
├── ITEM_BUSINESS_LINE_LOOKUP.csv   # Dimensione business line
│
├── output/                 # File di output generati automaticamente
│   ├── sales_merge.csv     # Tabella OLAP denormalizzata
│   ├── previsioni.csv      # Previsione ricavi 6 mesi
│   ├── istogramma.svg
│   ├── trend_ricavi.svg
│   ├── ordini_nazione.svg
│   ├── ricavi_azienda.svg
│   └── recency.svg
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
python -m venv venv
source venv/bin/activate        # macOS / Linux
venv\Scripts\activate           # Windows

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

1. Caricare i 6 file CSV
2. Cliccare su **"Avvia Pipeline Completa"** per eseguire l'intera pipeline ETL → Statistiche → Grafici → Previsioni
3. Scaricare il dataset OLAP (`Sales_OLAP_Completo.csv`) e le previsioni (`Sales_Forecast_PowerBI.csv`) dalla sezione di esportazione

### Opzione B — Importazione diretta dei moduli

Ogni modulo può essere utilizzato in modo autonomo:

```python
import pandas as pd
from etl import esegui_etl
from stat_mono import analisi_qualitativa, statistiche_numeriche, statistiche_categoriche
from grafici import plot_trend_ricavi_tempo, plot_rfm
from ForecastingLib import aggrega_dati, addestra_modello, genera_previsioni

# Esegui l'ETL completo e ottieni il DataFrame OLAP
olap = esegui_etl(salva=True)

# Esegui l'analisi statistica su ogni dataset
datasets = {"olap": olap}
for nome, df in datasets.items():
    analisi_qualitativa(df, nome)
    statistiche_numeriche(df, nome)
    statistiche_categoriche(df, nome)

# Genera i grafici
plot_trend_ricavi_tempo(olap)
plot_rfm(olap)

# Esegui il forecasting
aggregazioni = aggrega_dati(olap)
modello, metriche = addestra_modello(aggregazioni["mensile"])
previsioni = genera_previsioni(modello, aggregazioni["mensile"], n_mesi=6)
print(previsioni)
```

---

## Formato dei dati di input

Tutti e 6 i file CSV devono trovarsi nella stessa cartella degli script. Le colonne attese sono:

| File | Colonne principali |
|------|-------------------|
| `SALES.csv` | `ID_COMPANY`, `ID_ORDER_NUM`, `IDS_CUSTOMER`, `IDS_ITEM`, `ID_ORDER_DATE`, `ID_INVOICE_DATE`, `VAL_REVENUES`, `VAL_COST` |
| `CUSTOMER_LOOKUP.csv` | `IDS_CUSTOMER`, `DESC_CUSTOMER`, `ID_COUNTRY`, `ID_AREA_MANAGER` |
| `ITEM_LOOKUP.csv` | `IDS_ITEM`, `DESC_ITEM`, `ID_BUSINESS_LINE` |
| `COMPANY_LOOKUP.csv` | `ID_COMPANY`, `DESC_COMPANY` |
| `AREA_MANAGER_LOOKUP.csv` | `ID_AREA_MANAGER`, `DESC_AREA_MANAGER` |
| `ITEM_BUSINESS_LINE_LOOKUP.csv` | `ID_BUSINESS_LINE`, `DESC_BUSINESS_LINE` |

> **Nota:** le colonne data (`ID_ORDER_DATE`, `ID_INVOICE_DATE`) sono attese in formato intero `YYYYMMDD`. L'ETL gestisce la conversione e segnala eventuali anomalie (es. anno 2788 nei dati di test).

---

## Output generati

| File | Descrizione |
|------|-------------|
| `sales_merge.csv` | Tabella OLAP denormalizzata — tutti e 6 i dataset uniti in un unico file piatto, pronto per Power BI |
| `previsioni.csv` | Previsione ricavi a 6 mesi con colonne `DATE`, `FORECAST_REVENUES` e `TIPO` |
| `istogramma.svg` | Istogramma della distribuzione dei ricavi |
| `trend_ricavi.svg` | Andamento dei ricavi nel tempo |
| `ordini_nazione.svg` | Numero di ordini per nazione |
| `ricavi_azienda.svg` | Ricavi totali per azienda |
| `recency.svg` | Scatter plot RFM |

---

## Modello di previsione

Il modulo di forecasting (`ForecastingLib.py`) utilizza la **regressione lineare** (scikit-learn `LinearRegression`) con tre feature costruite ad hoc:

- `MONTH_IDX` — indice progressivo del mese, cattura il trend generale
- `ORDER_MONTH` — numero del mese (1–12), cattura la stagionalità
- `LAG_1` — ricavi del mese precedente, cattura il momentum a breve termine

La valutazione del modello utilizza **TimeSeriesSplit con 3 fold** per garantire che la validazione avvenga sempre su dati futuri rispetto al training. Dopo la valutazione, il modello viene riallenato su tutti i dati disponibili prima di generare le previsioni. Le previsioni future usano una **strategia autoregressiva** — ogni valore previsto diventa il `LAG_1` del passo successivo.

---

## Limitazioni note

- Il software non supporta formati di input diversi dal CSV: file Excel, JSON o database relazionali richiedono una fase di conversione preliminare prima di poter essere utilizzati.
- Il modulo di forecasting supporta esclusivamente l'aggregazione mensile: granularità giornaliera o settimanale non sono attualmente gestite.
