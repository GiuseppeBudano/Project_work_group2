import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import io # Aggiunto per gestire i file in memoria RAM

# Importo i tuoi moduli 
from modules import etl             
from modules import grafici
from modules import ForecastingLib  
from modules import stat_xlsx      


st.set_page_config(layout="wide")
def main():
    # 1. Intestazione con Logo e Titolo (proporzioni studiate per evitare il wrap)
    col_logo, col_titolo = st.columns([1, 20])       #prima era ([0.12, 0.88]) quando era centrato
    with col_logo:
        st.image("images/logo_cefla.png", width=85)
    with col_titolo:
        st.title("Analisi e Previsione Vendite")
    st.write("Carica il dataset storico per ETL, Statistiche, Grafici e Forecasting.")

    # Inizializzo la memoria di Streamlit
    if 'pipeline_completata' not in st.session_state:
        st.session_state.pipeline_completata = False

    # --- 1. UPLOAD DATI ---
    st.header("1. Caricamento Dati")
    file_caricati = st.file_uploader(
        "Carica i file CSV (es. SALES.csv, COMPANY_LOOKUP.csv, ecc.)", 
        type=["csv"], 
        accept_multiple_files=True
    )

    if file_caricati:
        st.success(f"Caricati {len(file_caricati)} file con successo.")
        
        # Salvataggio in RAM tramite dizionario
        tabelle = {}
        for file in file_caricati:
            df = pd.read_csv(file)
            nome = file.name.replace(".csv", "").replace(".CSV", "").upper()
            tabelle[nome] = df
               
        if st.button("Avvia Pipeline Completa (ETL -> Statistiche -> Grafici -> Forecast)"):
            with st.spinner("Elaborazione in corso..."):              
                
                # 1. ETL (passando il dizionario)
                df_olap = etl.main(carica=False, salva=False, tabelle=tabelle)
                st.session_state.df_olap = df_olap
                
                # 2. FORECASTING — vedi sezione dedicata più sotto
                aggregazioni = ForecastingLib.aggrega_dati(df_olap)

                modello_lr,  metriche_lr  = ForecastingLib.addestra_modello(aggregazioni["mensile"])
                modello_gbr, metriche_gbr = ForecastingLib.addestra_modello_gbr(aggregazioni["mensile"])

                previsioni_lr  = ForecastingLib.genera_previsioni(modello_lr,  aggregazioni["mensile"], n_mesi=6)
                previsioni_gbr = ForecastingLib.genera_previsioni(modello_gbr, aggregazioni["mensile"], n_mesi=6)
                previsioni_12  = ForecastingLib.genera_previsioni(modello_lr,  aggregazioni["mensile"], n_mesi=12)

                # Costruisce la tabella storico + previsioni LR per l'export Power BI
                storico = aggregazioni["mensile"].groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
                    FORECAST_REVENUES=("TOTAL_REVENUES", "sum")
                ).reset_index()
                storico["DATE"] = pd.to_datetime(
                    storico["ORDER_YEAR"].astype(str) + "-" +
                    storico["ORDER_MONTH"].astype(str).str.zfill(2) + "-01"
                )
                storico["TIPO"] = "STORICO"
                df_finale = pd.concat([storico, previsioni_lr], ignore_index=True)

                # Salva tutto in session_state per non ricalcolare al prossimo render
                st.session_state.aggregazioni   = aggregazioni
                st.session_state.modello_lr     = modello_lr
                st.session_state.modello_gbr    = modello_gbr
                st.session_state.metriche_lr    = metriche_lr
                st.session_state.metriche_gbr   = metriche_gbr
                st.session_state.previsioni_lr  = previsioni_lr
                st.session_state.previsioni_gbr = previsioni_gbr
                st.session_state.previsioni_12  = previsioni_12
                st.session_state.df_finale      = df_finale

                st.session_state.pipeline_completata = True
                st.rerun()

    # --- MOSTRA RISULTATI E PULSANTI ---
    if st.session_state.pipeline_completata:
        st.success("Dati trasformati e modelli applicati con successo!")
        
        # Recupero i dati dalla memoria
        df_olap        = st.session_state.df_olap
        df_finale      = st.session_state.df_finale
        aggregazioni   = st.session_state.aggregazioni
        metriche_lr    = st.session_state.metriche_lr
        metriche_gbr   = st.session_state.metriche_gbr
        previsioni_lr  = st.session_state.previsioni_lr
        previsioni_gbr = st.session_state.previsioni_gbr
        previsioni_12  = st.session_state.previsioni_12

        # --- 2. STATISTICHE (EXCEL IN MEMORIA) ---
        st.header("2. Statistiche Descrittive")
        
        # Creiamo un file virtuale in RAM per non innescare il riavvio di Streamlit
        excel_buffer = io.BytesIO()
        stat_xlsx.export_statistiche_excel({"SALES_OLAP": df_olap}, nome_file=excel_buffer)
        
        st.download_button(
            label="📊 Scarica Report Statistiche (Excel)", 
            data=excel_buffer.getvalue(), 
            file_name="report_statistiche.xlsx", 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Visualizzazione statistiche stat_mono nell'interfaccia
        with st.expander("Visualizza Analisi Qualitativa e Descrittiva"):
            st.subheader("Analisi Qualitativa")
            st.write(f"**Dimensioni:** {df_olap.shape[0]} righe x {df_olap.shape[1]} colonne")
            
            # Missing values (logica di analisi_qualitativa)
            missing = df_olap.isnull().sum()
            if missing.any():
                st.write("**Valori mancanti per colonna:**")
                st.dataframe(missing[missing > 0])
            
            # Statistiche Numeriche (logica di statistiche_numeriche)
            st.subheader("Statistiche Numeriche")
            var_escluse = ["ORDER_YEAR", "ORDER_MONTH", "ORDER_WEEK"]
            numeriche = df_olap.select_dtypes(include=np.number).drop(
                columns=[col for col in df_olap.columns if "ID" in col or col in var_escluse], 
                errors="ignore"
            )
            st.dataframe(numeriche.describe().T.round(2), use_container_width=True)

            # Statistiche Categoriche (logica di statistiche_categoriche)
            st.subheader("Distribuzione Variabili Categoriche (Top 10)")
            categoriche = df_olap.select_dtypes(include=["object", "category", "bool"])
            for col in categoriche.columns:
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.write(f"**{col}**")
                with col2:
                    res = pd.DataFrame({
                        "Frequenza": df_olap[col].value_counts().head(10),
                        "Percentuale (%)": (df_olap[col].value_counts(normalize=True).head(10)*100).round(2)
                    })
                    st.dataframe(res, use_container_width=True)
                    
        # --- 3. GRAFICI (CON PULIZIA MEMORIA) ---
        st.header("3. Analisi Visiva")
        grafici.imposta_stile()
        
        original_show = plt.show
        
        # Custom show per Streamlit che distrugge la figura dopo averla mostrata
        def st_show():
            st.pyplot(plt.gcf())
            plt.close('all') 
            
        plt.show = st_show
        
        grafici.plot_istogramma_ricavi(df_olap)
        grafici.plot_ricavi_per_azienda(df_olap)
        grafici.plot_trend_ricavi_tempo(df_olap)
        grafici.plot_heatmap_correlazioni(df_olap)
        grafici.plot_rfm(df_olap)
        
        plt.show = original_show

        # --- 4. FORECAST ---
        st.header("4. Generazione Previsioni")

        # ── 4a. Metriche di valutazione ──────────────────────────────────────
        st.subheader("4a. Metriche di valutazione (Cross-Validation)")
        st.write(
            "Le metriche sono calcolate con TimeSeriesSplit (3 fold): il modello "
            "viene sempre valutato su mesi che non ha mai visto durante il training."
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Regressione Lineare**")
            st.metric("MAE medio (€)",  f"{metriche_lr['MAE_medio_cv']:,.0f}")
            st.metric("RMSE medio (€)", f"{metriche_lr['RMSE_medio_cv']:,.0f}")
            st.metric("R² medio",       f"{metriche_lr['R2_medio_cv']:.4f}")

        with col2:
            st.markdown("**GradientBoosting**")
            st.metric("MAE medio (€)",  f"{metriche_gbr['MAE_medio_cv']:,.0f}")
            st.metric("RMSE medio (€)", f"{metriche_gbr['RMSE_medio_cv']:,.0f}")
            st.metric("R² medio",       f"{metriche_gbr['R2_medio_cv']:.4f}")

        # Dettaglio per fold — espandibile per non appesantire la pagina
        with st.expander("Mostra dettaglio per fold"):
            fold_data = {
                "Fold": [1, 2, 3],
                "MAE LR (€)":  [f"{v:,.0f}" for v in metriche_lr["mae_per_fold"]],
                "R² LR":       [f"{v:.4f}"  for v in metriche_lr["r2_per_fold"]],
                "MAE GBR (€)": [f"{v:,.0f}" for v in metriche_gbr["mae_per_fold"]],
                "R² GBR":      [f"{v:.4f}"  for v in metriche_gbr["r2_per_fold"]],
            }
            st.dataframe(pd.DataFrame(fold_data), use_container_width=True)

        # Importanza feature GBR
        with st.expander("Importanza delle feature (GradientBoosting)"):
            importanze = metriche_gbr.get("feature_importances", {})
            if importanze:
                df_imp = pd.DataFrame(
                    importanze.items(), columns=["Feature", "Importanza"]
                ).sort_values("Importanza", ascending=False)
                st.dataframe(df_imp, use_container_width=True)

        # ── 4b. Previsioni a 6 mesi — singolo modello ────────────────────────
        st.subheader("4b. Previsioni a 6 mesi — Regressione Lineare ")
        st.dataframe(df_finale)

        # --- 5. EXPORT FINALE ---
        st.header("5. Esportazione per Power BI")
        col1, col2 = st.columns(2)
        
        with col1:
            st.download_button(
                label="📈 Scarica Storico + Forecast (CSV)",
                data=df_finale.to_csv(index=False).encode('utf-8'),
                file_name="Sales_Forecast_PowerBI.csv",
                mime="text/csv"
            )
        with col2:
            st.download_button(
                label="🗄️ Scarica Dataset OLAP Completo (CSV)",
                data=df_olap.to_csv(index=False).encode('utf-8'),
                file_name="Sales_OLAP_Completo.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()