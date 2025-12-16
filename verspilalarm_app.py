import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
import io
import os
import calendar
import numpy as np


# --------------------------------------------------------
# Database setup
# --------------------------------------------------------
DB_PATH = "verspilalarmen.db"

def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table for alarm data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alarms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_date TEXT,
            ean_code TEXT,
            date TEXT,
            time TEXT,
            consumption REAL,
            temperature REAL,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER
        )
    """)
    
    # Check if hour column exists, if not add it
    cursor.execute("PRAGMA table_info(alarms)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'hour' not in columns:
        cursor.execute("ALTER TABLE alarms ADD COLUMN hour INTEGER")
    
    # Table for upload history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_date TEXT,
            filename_meetdata TEXT,
            filename_knmi TEXT,
            total_alarms INTEGER,
            unique_eans INTEGER
        )
    """)
    
    # Table for pattern filter settings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pattern_settings (
            id INTEGER PRIMARY KEY,
            continuous_threshold INTEGER DEFAULT 3,
            high_consumption_multiplier REAL DEFAULT 2.0,
            min_pattern_occurrences INTEGER DEFAULT 4,
            max_consumption_warm_water REAL DEFAULT 0.8,
            night_hour_start INTEGER DEFAULT 0,
            night_hour_end INTEGER DEFAULT 5,
            morning_peak_start INTEGER DEFAULT 6,
            morning_peak_end INTEGER DEFAULT 9,
            evening_peak_start INTEGER DEFAULT 17,
            evening_peak_end INTEGER DEFAULT 22
        )
    """)
    
    # Check and add new columns if they don't exist
    cursor.execute("PRAGMA table_info(pattern_settings)")
    settings_columns = [col[1] for col in cursor.fetchall()]
    
    new_columns = {
        'max_consumption_warm_water': 'REAL DEFAULT 0.8',
        'night_hour_start': 'INTEGER DEFAULT 0',
        'night_hour_end': 'INTEGER DEFAULT 5',
        'morning_peak_start': 'INTEGER DEFAULT 6',
        'morning_peak_end': 'INTEGER DEFAULT 9',
        'evening_peak_start': 'INTEGER DEFAULT 17',
        'evening_peak_end': 'INTEGER DEFAULT 22'
    }
    
    for col_name, col_type in new_columns.items():
        if col_name not in settings_columns:
            cursor.execute(f"ALTER TABLE pattern_settings ADD COLUMN {col_name} {col_type}")
    
    # Insert default settings if not exists
    cursor.execute("""
        INSERT OR IGNORE INTO pattern_settings 
        (id, continuous_threshold, high_consumption_multiplier, min_pattern_occurrences,
         max_consumption_warm_water, night_hour_start, night_hour_end,
         morning_peak_start, morning_peak_end, evening_peak_start, evening_peak_end) 
        VALUES (1, 3, 2.0, 4, 0.8, 0, 5, 6, 9, 17, 22)
    """)
    
    conn.commit()
    conn.close()

def extract_hour_from_time(time_str):
    """Extract hour from time string (format: HH:MM:SS or HH:MM)"""
    try:
        return int(str(time_str).split(':')[0])
    except:
        return 0

def save_alarms_to_db(alarms_df, meetdata_name, knmi_name):
    """Save alarm data to database"""
    conn = sqlite3.connect(DB_PATH)
    upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _, row in alarms_df.iterrows():
        if isinstance(row['Date'], str):
            try:
                date_obj = datetime.strptime(row['Date'], "%d-%m-%Y")
            except:
                try:
                    date_obj = datetime.strptime(row['Date'], "%Y-%m-%d")
                except:
                    date_obj = pd.to_datetime(row['Date'], dayfirst=True)
        else:
            date_obj = row['Date']
        
        hour = extract_hour_from_time(row['Time'])
        
        conn.execute("""
            INSERT INTO alarms (upload_date, ean_code, date, time, consumption, temperature, year, month, day, hour)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (upload_date, row['metereancode'], row['Date'], row['Time'], 
              row['Consumption'], row['T'], date_obj.year, date_obj.month, date_obj.day, hour))
    
    conn.execute("""
        INSERT INTO uploads (upload_date, filename_meetdata, filename_knmi, total_alarms, unique_eans)
        VALUES (?, ?, ?, ?, ?)
    """, (upload_date, meetdata_name, knmi_name, len(alarms_df), alarms_df['metereancode'].nunique()))
    
    conn.commit()
    conn.close()
    return upload_date

def ensure_datetime_columns(df):
    """Ensure year, month, day, hour columns exist in dataframe"""
    df = df.copy()
    
    if 'hour' not in df.columns or df['hour'].isna().any():
        df['hour'] = df['time'].apply(extract_hour_from_time)
    
    if 'year' not in df.columns or df['year'].isna().any():
        try:
            dates = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
            df['year'] = dates.dt.year
            df['month'] = dates.dt.month
            df['day'] = dates.dt.day
        except:
            pass
    
    return df

def analyze_consumption_pattern(ean_data, settings):
    """Analyze if consumption pattern indicates waste or warm water usage"""
    if len(ean_data) < settings.get('min_pattern_occurrences', 4):
        return True
    
    ean_data = ean_data.copy()
    ean_data = ensure_datetime_columns(ean_data)
    
    max_consumption_warm = settings.get('max_consumption_warm_water', 0.8)
    night_start = settings.get('night_hour_start', 0)
    night_end = settings.get('night_hour_end', 5)
    morning_start = settings.get('morning_peak_start', 6)
    morning_end = settings.get('morning_peak_end', 9)
    evening_start = settings.get('evening_peak_start', 17)
    evening_end = settings.get('evening_peak_end', 22)
    continuous_threshold = settings.get('continuous_threshold', 3)
    
    consumptions = ean_data['consumption'].values
    mean_consumption = np.mean(consumptions)
    total_records = len(ean_data)
    
    waste_score = 0
    warm_water_score = 0
    
    # 1. CONSUMPTION ANALYSIS
    if mean_consumption < 0.3:
        warm_water_score += 3
    elif mean_consumption < max_consumption_warm:
        warm_water_score += 2
    elif mean_consumption > 1.5:
        waste_score += 3
    elif mean_consumption > 1.0:
        waste_score += 2
    
    small_consumption_ratio = np.sum(consumptions < max_consumption_warm) / total_records
    if small_consumption_ratio > 0.8:
        warm_water_score += 2
    elif small_consumption_ratio > 0.6:
        warm_water_score += 1
    elif small_consumption_ratio < 0.3:
        waste_score += 2
    
    # 2. TIME OF DAY ANALYSIS
    hours = ean_data['hour'].values
    
    night_ratio = np.sum((hours >= night_start) & (hours <= night_end)) / total_records
    if night_ratio > 0.3:
        waste_score += 3
    elif night_ratio > 0.15:
        waste_score += 2
    elif night_ratio < 0.05:
        warm_water_score += 1
    
    morning_ratio = np.sum((hours >= morning_start) & (hours <= morning_end)) / total_records
    if morning_ratio > 0.25:
        warm_water_score += 2
    elif morning_ratio > 0.15:
        warm_water_score += 1
    
    evening_ratio = np.sum((hours >= evening_start) & (hours <= evening_end)) / total_records
    if evening_ratio > 0.3:
        warm_water_score += 2
    elif evening_ratio > 0.2:
        warm_water_score += 1
    
    peak_ratio = morning_ratio + evening_ratio
    if peak_ratio > 0.5:
        warm_water_score += 2
    
    # 3. CONTINUITY ANALYSIS
    sort_columns = [col for col in ['year', 'month', 'day', 'hour'] if col in ean_data.columns]
    if sort_columns:
        ean_data_sorted = ean_data.sort_values(sort_columns).copy()
    else:
        ean_data_sorted = ean_data.copy()
    
    current_continuous = 1
    continuous_streaks = []
    
    prev_hour = None
    prev_day = None
    prev_month = None
    
    for _, row in ean_data_sorted.iterrows():
        current_hour = row.get('hour', 0)
        current_day = row.get('day', 0)
        current_month = row.get('month', 0)
        
        if prev_hour is not None:
            same_day = (prev_day == current_day and prev_month == current_month)
            next_hour = (current_hour == prev_hour + 1) or (prev_hour == 23 and current_hour == 0)
            
            if same_day and next_hour:
                current_continuous += 1
            else:
                if current_continuous >= 2:
                    continuous_streaks.append(current_continuous)
                current_continuous = 1
        
        prev_hour = current_hour
        prev_day = current_day
        prev_month = current_month
    
    if current_continuous >= 2:
        continuous_streaks.append(current_continuous)
    
    max_continuous = max(continuous_streaks) if continuous_streaks else 1
    num_long_streaks = sum(1 for s in continuous_streaks if s >= continuous_threshold)
    
    if max_continuous >= 6:
        waste_score += 4
    elif max_continuous >= continuous_threshold:
        waste_score += 2
    elif max_continuous <= 2:
        warm_water_score += 2
    
    if num_long_streaks >= 5:
        waste_score += 3
    elif num_long_streaks >= 3:
        waste_score += 2
    
    # 4. DAY SPREAD ANALYSIS
    unique_days = ean_data[['year', 'month', 'day']].drop_duplicates()
    num_unique_days = len(unique_days)
    records_per_day = total_records / max(num_unique_days, 1)
    
    if records_per_day > 8:
        waste_score += 3
    elif records_per_day > 5:
        waste_score += 2
    elif records_per_day < 3:
        warm_water_score += 2
    
    # 5. HOUR SPREAD ANALYSIS
    unique_hours = len(ean_data['hour'].unique())
    
    if unique_hours >= 10 and mean_consumption < max_consumption_warm:
        warm_water_score += 2
    
    if unique_hours <= 5 and mean_consumption > 1.0:
        waste_score += 2
    
    # 6. VARIANCE ANALYSIS
    std_consumption = np.std(consumptions)
    cv = std_consumption / mean_consumption if mean_consumption > 0 else 0
    
    if cv > 0.5 and mean_consumption < max_consumption_warm:
        warm_water_score += 2
    
    if cv < 0.3 and mean_consumption > 0.8:
        waste_score += 2
    
    # FINAL DECISION
    net_score = waste_score - warm_water_score
    
    if net_score <= 0:
        return False
    else:
        return True

def load_all_alarms(apply_pattern_filter=True):
    """Load all alarms from database with optional pattern-based filtering"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM alarms ORDER BY year DESC, month DESC, day DESC", conn)
    
    if not df.empty:
        df = ensure_datetime_columns(df)
    
    if apply_pattern_filter and not df.empty:
        settings = pd.read_sql_query("SELECT * FROM pattern_settings WHERE id = 1", conn)
        
        if not settings.empty:
            settings_dict = settings.iloc[0].to_dict()
            filtered_alarms = []
            
            for ean_code in df['ean_code'].unique():
                ean_data = df[df['ean_code'] == ean_code].copy()
                is_waste = analyze_consumption_pattern(ean_data, settings_dict)
                
                if is_waste:
                    filtered_alarms.append(ean_data)
            
            if filtered_alarms:
                df = pd.concat(filtered_alarms, ignore_index=True)
            else:
                df = pd.DataFrame()
    
    conn.close()
    return df

def get_pattern_settings():
    """Get current pattern filter settings"""
    conn = sqlite3.connect(DB_PATH)
    settings = pd.read_sql_query("SELECT * FROM pattern_settings WHERE id = 1", conn)
    conn.close()
    return settings.iloc[0] if not settings.empty else None

def update_pattern_settings(settings_dict):
    """Update pattern filter settings"""
    conn = sqlite3.connect(DB_PATH)
    
    set_clauses = []
    values = []
    for key, value in settings_dict.items():
        if key != 'id':
            set_clauses.append(f"{key} = ?")
            values.append(value)
    
    values.append(1)
    
    query = f"UPDATE pattern_settings SET {', '.join(set_clauses)} WHERE id = ?"
    conn.execute(query, values)
    conn.commit()
    conn.close()

def get_pattern_analysis_summary(df):
    """Get summary of pattern analysis for each EAN"""
    settings = get_pattern_settings()
    if settings is None or df.empty:
        return pd.DataFrame()
    
    settings_dict = settings.to_dict()
    df = ensure_datetime_columns(df)
    
    summary_data = []
    
    for ean_code in df['ean_code'].unique():
        ean_data = df[df['ean_code'] == ean_code].copy()
        
        mean_consumption = ean_data['consumption'].mean()
        std_consumption = ean_data['consumption'].std()
        cv = std_consumption / mean_consumption if mean_consumption > 0 else 0
        
        hours = ean_data['hour'].values
        night_ratio = np.sum((hours >= 0) & (hours <= 5)) / len(hours) * 100
        morning_ratio = np.sum((hours >= 6) & (hours <= 9)) / len(hours) * 100
        evening_ratio = np.sum((hours >= 17) & (hours <= 22)) / len(hours) * 100
        
        is_waste = analyze_consumption_pattern(ean_data, settings_dict)
        pattern_type = "🔴 Verspilling" if is_waste else "🟢 Warm water"
        
        unique_hours = ean_data['hour'].nunique()
        
        summary_data.append({
            'EAN-code': ean_code,
            'Patroon': pattern_type,
            'Alarmen': len(ean_data),
            'Gem. m³': round(mean_consumption, 3),
            'Nacht %': round(night_ratio, 1),
            'Ochtend %': round(morning_ratio, 1),
            'Avond %': round(evening_ratio, 1),
            'Uur spread': unique_hours
        })
    
    return pd.DataFrame(summary_data).sort_values('Alarmen', ascending=False)

def load_uploads_history():
    """Load upload history"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM uploads ORDER BY upload_date DESC", conn)
    conn.close()
    return df

def delete_upload_data(upload_date):
    """Delete data from a specific upload"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM alarms WHERE upload_date = ?", (upload_date,))
    conn.execute("DELETE FROM uploads WHERE upload_date = ?", (upload_date,))
    conn.commit()
    conn.close()

def clear_all_data():
    """Clear all data from database"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM alarms")
    conn.execute("DELETE FROM uploads")
    conn.commit()
    conn.close()

def detect_gas_waste(meetdata_file, knmi_file):
    """Detect gas waste from uploaded files"""
    try:
        df = pd.read_csv(meetdata_file, sep=";", engine="python")
        
        required_cols = ["Date", "Time", "Consumption", "metereancode"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            return None, f"Ontbrekende kolommen in meetdata: {', '.join(missing)}"
        
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce', dayfirst=True)
        df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str), errors='coerce')
        df = df[["metereancode", "DateTime", "Consumption"]].dropna()
        df["Consumption"] = pd.to_numeric(df["Consumption"], errors='coerce')

        knmi = pd.read_csv(
            knmi_file,
            comment="#",
            header=None,
            names=["STN", "YYYYMMDD", "HH", "T"],
            usecols=["YYYYMMDD", "HH", "T"],
            engine="python",
        )
        knmi["T"] = knmi["T"] / 10
        knmi["Date"] = pd.to_datetime(knmi["YYYYMMDD"], format="%Y%m%d")

        mask = knmi["HH"] == 24
        knmi.loc[mask, "HH"] = 0
        knmi.loc[mask, "Date"] += pd.Timedelta(days=1)

        knmi["Time"] = knmi["HH"].astype(str).str.zfill(2) + ":00:00"
        knmi["DateTime"] = pd.to_datetime(
            knmi["Date"].dt.strftime("%d-%m-%Y") + " " + knmi["Time"],
            dayfirst=True,
            errors='coerce'
        )
        knmi = knmi[["DateTime", "T"]].dropna()

        merged = pd.merge(df, knmi, on="DateTime", how="inner")
        alarms = merged[(merged["Consumption"] > 0) & (merged["T"] > 20)].copy()

        if alarms.empty:
            return None, None

        alarms["Date"] = alarms["DateTime"].dt.strftime("%d-%m-%Y")
        alarms["Time"] = alarms["DateTime"].dt.strftime("%H:%M")
        alarms["T"] = alarms["T"].round(1)

        return alarms, None

    except Exception as e:
        return None, f"Fout bij verwerking: {str(e)}"

def create_summary_with_frequency(alarms_df):
    summary = alarms_df.groupby("ean_code").agg({
        "consumption": ["count", "sum", "mean"]
    }).round(2)
    
    summary.columns = ["Aantal alarmen", "Totaal verbruik (m³)", "Gem. verbruik (m³)"]
    summary = summary.reset_index()
    summary.columns = ["EAN-code", "Aantal alarmen", "Totaal verbruik (m³)", "Gem. verbruik (m³)"]
    
    return summary.sort_values("Aantal alarmen", ascending=False)

def create_monthly_overview(alarms_df, ean_code):
    ean_data = alarms_df[alarms_df['ean_code'] == ean_code].copy()
    
    if ean_data.empty:
        return None
    
    monthly = ean_data.groupby(['year', 'month']).agg({
        'consumption': ['count', 'sum']
    }).reset_index()
    
    monthly.columns = ['Jaar', 'Maand', 'Aantal alarmen', 'Totaal verbruik (m³)']
    monthly['Maand naam'] = monthly['Maand'].apply(lambda x: calendar.month_name[x])
    monthly['Totaal verbruik (m³)'] = monthly['Totaal verbruik (m³)'].round(2)
    
    return monthly[['Jaar', 'Maand naam', 'Aantal alarmen', 'Totaal verbruik (m³)']]

def create_excel_report(alarms_df):
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        summary = create_summary_with_frequency(alarms_df)
        summary.to_excel(writer, sheet_name='Samenvatting per EAN', index=False)
        
        detail = alarms_df[['ean_code', 'date', 'time', 'consumption', 'year', 'month']].copy()
        detail.columns = ['EAN-code', 'Datum', 'Tijd', 'Verbruik (m³)', 'Jaar', 'Maand']
        detail.to_excel(writer, sheet_name='Alle alarmen', index=False)
    
    output.seek(0)
    return output

# Initialize database
init_database()

# --------------------------------------------------------
# Page configuration
# --------------------------------------------------------
st.set_page_config(
    page_title="Verspilalarmen 's Heeren Loo",
    layout="wide",
)

# --------------------------------------------------------
# CSS styling
# --------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');

* {
    font-family: 'Roboto', sans-serif;
}

.block-container {
    padding: 1rem 2rem;
    max-width: 1400px;
}

h1, h2, h3 {
    color: #558B2F;
    font-weight: 500;
}

.stButton > button {
    background: linear-gradient(135deg, #7CB342 0%, #558B2F 100%);
    color: white;
    border: none;
    padding: 0.6rem 1.5rem;
    border-radius: 6px;
    font-weight: 500;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background-color: #f1f8e9;
    padding: 0.5rem;
    border-radius: 8px;
}

.stTabs [data-baseweb="tab"] {
    background-color: white;
    color: #558B2F;
    border-radius: 6px;
    padding: 0.5rem 1.5rem;
    font-weight: 500;
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #7CB342 0%, #558B2F 100%);
    color: white;
}

.metric-card {
    background: white;
    padding: 1.5rem;
    border-radius: 12px;
    text-align: center;
    border: 2px solid #7CB342;
    box-shadow: 0 2px 8px rgba(124, 179, 66, 0.15);
}

.metric-card h3 {
    color: #558B2F;
    margin: 0;
    font-size: 2rem;
    font-weight: 700;
}

.metric-card p {
    color: #757575;
    margin: 0.5rem 0 0 0;
    font-size: 0.9rem;
}

.alarm-card {
    background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
    border-left: 5px solid #ff9800;
    padding: 1.2rem;
    margin: 0.8rem 0;
    border-radius: 8px;
}

.success-card {
    background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
    border-left: 5px solid #28a745;
    padding: 1.2rem;
    margin: 0.8rem 0;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------------
# Title
# --------------------------------------------------------
st.markdown("""
<div style='background: linear-gradient(135deg, #7CB342 0%, #558B2F 100%); 
            padding: 2rem; 
            border-radius: 12px; 
            margin-bottom: 2rem;
            box-shadow: 0 4px 12px rgba(85, 139, 47, 0.3);'>
    <h1 style='color: white; margin-bottom:0.5rem; text-align: center;'>
         Verspilalarmen Dashboard
    </h1>
    <p style='color: rgba(255,255,255,0.9); margin:0; text-align: center; font-size: 1.1rem;'>
        's Heeren Loo | Energiebeheer Monitoring
    </p>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------------
# Sidebar
# --------------------------------------------------------
st.sidebar.title("🔔 Verspilalarmen")
alarm_type = st.sidebar.radio(
    "Selecteer alarm type:",
    [
        "Verspilalarm 1",
        "Verspilalarm 2 (Gas boven 20°C)",
        "Verspilalarm 3",
        "Verspilalarm 4",
        "Verspilalarm 5",
        "Verspilalarm 6",
        "Verspilalarm 7"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style='padding: 1rem; background: rgba(255,255,255,0.1); border-radius: 8px;'>
    <p style='color: white; font-size: 0.85rem; margin: 0;'>
        <strong>Energiemissie</strong><br>
        Verspilalarmen v3.2
    </p>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------------
# VERSPILALARM 2
# --------------------------------------------------------
if alarm_type == "Verspilalarm 2 (Gas boven 20°C)":
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard", 
        "📈 Jaaroverzicht", 
        "🔍 Top verspillers", 
        "📜 Geschiedenis", 
        "⚙️ Instellingen"
    ])
    
    # ========================================
    # TAB 1: DASHBOARD
    # ========================================
    with tab1:
        st.subheader("📊 Dashboard overzicht")
        
        settings = get_pattern_settings()
        if settings is not None:
            st.info("🤖 **Verbeterde patroonherkenning actief:** Warm water wordt herkend op basis van tijdstip, verbruikshoogte en continuïteit")
        
        all_alarms = load_all_alarms(apply_pattern_filter=True)
        all_alarms_unfiltered = load_all_alarms(apply_pattern_filter=False)
        
        if all_alarms_unfiltered.empty:
            st.warning(" Nog geen data. Ga naar **Instellingen** om bestanden te uploaden.")
        else:
            filtered_count = len(all_alarms_unfiltered) - len(all_alarms)
            filtered_eans = all_alarms_unfiltered['ean_code'].nunique() - all_alarms['ean_code'].nunique() if not all_alarms.empty else all_alarms_unfiltered['ean_code'].nunique()
            
            if filtered_count > 0:
                st.success(f"🟢 **{filtered_count} alarmen** ({filtered_eans} EAN's) geïdentificeerd als warm water en uitgefilterd")
            
            if all_alarms.empty:
                st.markdown("""
                <div class='success-card'>
                    <h4 style='margin:0; color: #155724;'>✅ Geen verspilling gedetecteerd</h4>
                    <p style='margin:0.5rem 0 0 0;'>
                        Alle alarmen zijn geïdentificeerd als normaal warm water verbruik.
                    </p>
                </div>
                """, unsafe_allow_html=True)
            else:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{len(all_alarms)}</h3>
                        <p>Verspillingsalarmen</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{all_alarms['ean_code'].nunique()}</h3>
                        <p>Aansluitingen met verspilling</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{all_alarms['consumption'].sum():.1f} m³</h3>
                        <p>Totaal verspild</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                
                st.subheader("📊 Overzicht per aansluiting (EAN)")
                summary = create_summary_with_frequency(all_alarms)
                st.dataframe(summary, hide_index=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    excel_report = create_excel_report(all_alarms)
                    st.download_button(
                        label="📊 Download volledig rapport (Excel)",
                        data=excel_report,
                        file_name=f"verspilalarmen_rapport_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )

    # ========================================
    # TAB 2: JAAROVERZICHT
    # ========================================
    with tab2:
        st.subheader("📈 Jaaroverzicht per EAN-code")
        
        all_alarms = load_all_alarms(apply_pattern_filter=True)
        
        if all_alarms.empty:
            st.info("ℹ️ Nog geen verspillingsdata na filtering.")
        else:
            unique_eans = sorted(all_alarms['ean_code'].unique())
            selected_ean = st.selectbox("Selecteer EAN-code", unique_eans, key="year_ean_select")
            
            available_years = sorted(all_alarms[all_alarms['ean_code'] == selected_ean]['year'].unique(), reverse=True)
            selected_year = st.selectbox("Selecteer jaar", available_years, key="year_select")
            
            st.markdown("---")
            
            monthly_data = create_monthly_overview(all_alarms, selected_ean)
            
            if monthly_data is not None:
                year_data = monthly_data[monthly_data['Jaar'] == selected_year]
                
                total_alarms = year_data['Aantal alarmen'].sum()
                total_consumption = year_data['Totaal verbruik (m³)'].sum()
                avg_per_month = year_data['Aantal alarmen'].mean()
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{total_alarms}</h3>
                        <p>Totaal alarmen ({selected_year})</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{total_consumption:.1f} m³</h3>
                        <p>Totaal verspild ({selected_year})</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <h3>{avg_per_month:.1f}</h3>
                        <p>Gem. per maand</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                
                st.subheader(f"📅 Maandoverzicht {selected_year} - EAN: {selected_ean}")
                st.dataframe(year_data, hide_index=True)
                
                csv_monthly = year_data.to_csv(index=False, sep=";").encode('utf-8')
                st.download_button(
                    label=f"📥 Download maandoverzicht {selected_year} (CSV)",
                    data=csv_monthly,
                    file_name=f"maandoverzicht_{selected_ean}_{selected_year}.csv",
                    mime="text/csv"
                )

    # ========================================
    # TAB 3: TOP VERSPILLERS
    # ========================================
    with tab3:
        st.subheader("🔍 Top verspillers")
        
        all_alarms = load_all_alarms(apply_pattern_filter=True)
        
        if all_alarms.empty:
            st.info("ℹ️ Geen verspilling gedetecteerd na filtering.")
        else:
            st.markdown("### 🔴 Top 10 aansluitingen met meeste alarmen")
            
            top_eans = all_alarms.groupby('ean_code').agg({
                'consumption': ['count', 'sum']
            }).round(2)
            top_eans.columns = ['Aantal alarmen', 'Totaal verbruik (m³)']
            top_eans = top_eans.reset_index().sort_values('Aantal alarmen', ascending=False).head(10)
            top_eans.columns = ['EAN-code', 'Aantal alarmen', 'Totaal verbruik (m³)']
            top_eans.insert(0, 'Rank', range(1, len(top_eans) + 1))
            
            st.dataframe(top_eans, hide_index=True)
            
            st.markdown("---")
            st.markdown("### EAN's die aandacht vereisen")
            
            summary = create_summary_with_frequency(all_alarms)
        
            csv_top = top_eans.to_csv(index=False, sep=";").encode('utf-8')
            st.download_button(
                label="📥 Download top verspillers (CSV)",
                data=csv_top,
                file_name=f"top_verspillers_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

    # ========================================
    # TAB 4: GESCHIEDENIS
    # ========================================
    with tab4:
        st.subheader("📜 Upload geschiedenis")
        
        history = load_uploads_history()
        
        if history.empty:
            st.info("ℹ️ Nog geen uploads gevonden.")
        else:
            st.write(f"**Totaal aantal uploads:** {len(history)}")
            
            display_history = history.copy()
            display_history.columns = ['ID', 'Upload datum', 'Meetdata bestand', 'KNMI bestand', 'Totaal alarmen', 'Unieke EAN\'s']
            st.dataframe(display_history, hide_index=True)
            
            st.markdown("---")
            st.subheader("🗑️ Verwijder upload")
            
            upload_dates = history['upload_date'].tolist()
            selected_delete = st.selectbox(
                "Selecteer upload om te verwijderen",
                ["Selecteer..."] + upload_dates,
                key="delete_upload_select"
            )
            
            if selected_delete != "Selecteer...":
                selected_info = history[history['upload_date'] == selected_delete].iloc[0]
                st.warning(f"""
                **Let op:** Je staat op het punt om de volgende upload te verwijderen:
                - Datum: {selected_delete}
                - Alarmen: {selected_info['total_alarms']}
                """)
                
                if st.button("🗑️ Verwijder deze upload", type="primary"):
                    delete_upload_data(selected_delete)
                    st.success("✅ Upload succesvol verwijderd!")
                    st.rerun()

    # ========================================
    # TAB 5: INSTELLINGEN
    # ========================================
    with tab5:
        st.subheader("⚙️ Instellingen")
        
        # ----------------------------------------
        # SECTIE 1: DATA UPLOADEN
        # ----------------------------------------
        st.markdown("### 📤 Data uploaden")
        st.info("Upload hier nieuwe meetdata.")
        
        col1, col2 = st.columns(2)
        with col1:
            meetdata_file = st.file_uploader(
                "Meetdata (CSV met ; als scheidingsteken)", 
                type="csv",
                key="meetdata_upload"
            )
        with col2:
            knmi_file = st.file_uploader(
                "KNMI data (TXT)", 
                type="txt",
                key="knmi_upload"
            )

        if st.button("🔍 Analyseer en opslaan", use_container_width=True, type="primary"):
            if meetdata_file is None or knmi_file is None:
                st.warning(" Upload beide bestanden om te starten.")
            else:
                with st.spinner("⏳ Bezig met analyse..."):
                    result, error = detect_gas_waste(meetdata_file, knmi_file)

                if error:
                    st.error(f"❌ Fout: {error}")
                elif result is None or result.empty:
                    st.success("✅ Geen alarmen gevonden in de nieuwe data.")
                else:
                    upload_date = save_alarms_to_db(result, meetdata_file.name, knmi_file.name)
                    st.success(f"✅ Analyse opgeslagen! {len(result)} alarmen toegevoegd.")
                    st.rerun()
        
        st.markdown("---")
        
        # ----------------------------------------
        # SECTIE 2: PATROONHERKENNING
        # ----------------------------------------
        st.markdown("### 🤖 Patroonherkenning instellingen")
        st.info("""
        **Warm water vs Verspilling detectie:**
        - 🟢 Warm water: Laag verbruik, ochtend/avond, korte pieken
        - 🔴 Verspilling: Hoog verbruik, 's nachts, continue patronen
        """)
        
        settings = get_pattern_settings()
        if settings is not None:
            col1, col2 = st.columns(2)
            
            with col1:
                continuous_threshold = st.number_input(
                    "Continue verbruik drempel (uren)",
                    min_value=2, max_value=10,
                    value=int(settings.get('continuous_threshold', 3))
                )
                
                min_pattern_occurrences = st.number_input(
                    "Minimaal aantal meetpunten",
                    min_value=3, max_value=20,
                    value=int(settings.get('min_pattern_occurrences', 4))
                )
                
                max_consumption_warm = st.number_input(
                    "Max verbruik warm water (m³)",
                    min_value=0.3, max_value=2.0,
                    value=float(settings.get('max_consumption_warm_water', 0.8)),
                    step=0.1
                )
            
            with col2:
                high_consumption_multiplier = st.number_input(
                    "Hoog verbruik factor",
                    min_value=1.5, max_value=5.0,
                    value=float(settings.get('high_consumption_multiplier', 2.0)),
                    step=0.1
                )
            
            st.markdown("#### 🕐 Tijdsinstellingen")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**Nacht (verspilling)**")
                night_start = st.number_input("Start", min_value=0, max_value=23, 
                                              value=int(settings.get('night_hour_start', 0)), key="ns")
                night_end = st.number_input("Eind", min_value=0, max_value=23, 
                                            value=int(settings.get('night_hour_end', 5)), key="ne")
            
            with col2:
                st.markdown("**Ochtend (warm water)**")
                morning_start = st.number_input("Start", min_value=0, max_value=23, 
                                                value=int(settings.get('morning_peak_start', 6)), key="ms")
                morning_end = st.number_input("Eind", min_value=0, max_value=23, 
                                              value=int(settings.get('morning_peak_end', 9)), key="me")
            
            with col3:
                st.markdown("**Avond (warm water)**")
                evening_start = st.number_input("Start", min_value=0, max_value=23, 
                                                value=int(settings.get('evening_peak_start', 17)), key="es")
                evening_end = st.number_input("Eind", min_value=0, max_value=23, 
                                              value=int(settings.get('evening_peak_end', 22)), key="ee")
            
            if st.button("💾 Instellingen opslaan", type="primary"):
                new_settings = {
                    'continuous_threshold': continuous_threshold,
                    'high_consumption_multiplier': high_consumption_multiplier,
                    'min_pattern_occurrences': min_pattern_occurrences,
                    'max_consumption_warm_water': max_consumption_warm,
                    'night_hour_start': night_start,
                    'night_hour_end': night_end,
                    'morning_peak_start': morning_start,
                    'morning_peak_end': morning_end,
                    'evening_peak_start': evening_start,
                    'evening_peak_end': evening_end
                }
                update_pattern_settings(new_settings)
                st.success("✅ Instellingen opgeslagen!")
                st.rerun()
        
        st.markdown("---")
        
        # ----------------------------------------
        # SECTIE 3: PATROONANALYSE OVERZICHT
        # ----------------------------------------
        st.markdown("### 📊 Patroonanalyse overzicht")
        all_data = load_all_alarms(apply_pattern_filter=False)
        
        if not all_data.empty:
            pattern_summary = get_pattern_analysis_summary(all_data)
            
            if not pattern_summary.empty:
                st.dataframe(pattern_summary, hide_index=True, use_container_width=True)
                
                waste_count = len(pattern_summary[pattern_summary['Patroon'].str.contains('Verspilling')])
                warm_water_count = len(pattern_summary[pattern_summary['Patroon'].str.contains('Warm water')])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🔴 Verspilling", waste_count)
                with col2:
                    st.metric("🟢 Warm water", warm_water_count)
                with col3:
                    st.metric("Totaal EAN's", len(pattern_summary))
        else:
            st.info("Nog geen data om te analyseren")
        
        st.markdown("---")
        
        # ----------------------------------------
        # SECTIE 4: DATABASE BEHEER
        # ----------------------------------------
        st.markdown("### 🗑️ Database beheer")
        
        all_alarms_unfiltered = load_all_alarms(apply_pattern_filter=False)
        all_alarms_filtered = load_all_alarms(apply_pattern_filter=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Totaal alarmen", len(all_alarms_unfiltered))
            st.metric("Verspillingsalarmen", len(all_alarms_filtered))
        with col2:
            if os.path.exists(DB_PATH):
                db_size = os.path.getsize(DB_PATH) / 1024
                st.metric("Database grootte", f"{db_size:.2f} KB")
            filtered_out = len(all_alarms_unfiltered) - len(all_alarms_filtered)
            st.metric("Warm water (uitgefilterd)", filtered_out)
        
        st.warning(" **Waarschuwing:** Verwijdert ALLE data permanent!")
        
        confirm = st.checkbox("Ik begrijp dat alle data verwijderd wordt", key="confirm_delete")
        
        if confirm:
            if st.button("🗑️ Verwijder alle data", type="primary"):
                clear_all_data()
                st.success("✅ Alle data verwijderd!")
                st.rerun()

# --------------------------------------------------------
# OTHER ALARMS
# --------------------------------------------------------
else:
    st.info(f"🚧 **{alarm_type}** is nog in ontwikkeling.")

# --------------------------------------------------------
# Footer
# --------------------------------------------------------
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #558B2F; padding: 1.5rem; 
            background: linear-gradient(135deg, #f1f8e9 0%, #dcedc8 100%); 
            border-radius: 8px; margin-top: 2rem;'>
    <p style='margin: 0; font-weight: 500;'>
        Verspilalarmen Dashboard v3.2 | 's Heeren Loo Energiebeheer
    </p>
</div>
""", unsafe_allow_html=True)