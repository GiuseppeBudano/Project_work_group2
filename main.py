import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import io # Aggiunto per gestire i file in memoria RAM

# Importo i tuoi moduli 
import etl             
import grafici
import ForecastingLib  
import stat_xlsx      

def main():
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
        
        # Salvataggio locale per farli leggere a etl.py (da eliminare in futuro)
        for file in file_caricati:
            with open(file.name, "wb") as f:
                f.write(file.getbuffer())
        #in futuro potremmo voler standardizzare i nomi delle tabelle in modo più robusto
               
        if st.button("Avvia Pipeline Completa (ETL -> Statistiche -> Grafici -> Forecast)"):
            with st.spinner("Elaborazione in corso..."):              
                
    
                # 1. ETL (passando il dizionario)
                df_olap = etl.esegui_etl(salva=False)
                st.session_state.df_olap = df_olap
                
                # 2. FORECAST
                aggregazioni = ForecastingLib.aggrega_dati(df_olap)
                modello, metriche = ForecastingLib.addestra_modello(aggregazioni["mensile"])
                previsioni = ForecastingLib.genera_previsioni(modello, aggregazioni["mensile"], n_mesi=6)
                
                storico = aggregazioni["mensile"].groupby(["ORDER_YEAR", "ORDER_MONTH"]).agg(
                    FORECAST_REVENUES=("TOTAL_REVENUES", "sum")
                ).reset_index()
                
                storico["DATE"] = pd.to_datetime(storico["ORDER_YEAR"].astype(str) + "-" + storico["ORDER_MONTH"].astype(str).str.zfill(2) + "-01")
                storico["TIPO"] = "STORICO"
                
                df_finale = pd.concat([storico, previsioni], ignore_index=True)
                st.session_state.df_finale = df_finale
                
                # Segno che i calcoli sono finiti e ricarico
                st.session_state.pipeline_completata = True
                st.rerun()

    # --- MOSTRA RISULTATI E PULSANTI ---
    if st.session_state.pipeline_completata:
        st.success("Dati trasformati e modelli applicati con successo!")
        
        # Recupero i dati dalla memoria
        df_olap = st.session_state.df_olap
        df_finale = st.session_state.df_finale

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
        grafici.plot_trend_ricavi_tempo(df_olap)
        
        plt.show = original_show

        # --- 4. FORECAST ---
        st.header("4. Generazione Previsioni")
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