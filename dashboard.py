import streamlit as st
import json
import pandas as pd
import time
import subprocess
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import altair as alt

# ---------------------------------------------------------
# 1. PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="Energy AI Command Center", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Professional Dark UI Styling
st.markdown("""
<style>
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 1px solid #464b5c;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    h1 { font-family: 'Helvetica Neue', sans-serif; font-weight: 700; color: white; }
    h3 { color: #cfcfcf; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. SIDEBAR & HEADER
# ---------------------------------------------------------
with st.sidebar:
    st.header("🎮 Control Panel")
    if st.button("🔄 Refresh Forecast Data", use_container_width=True):
        try:
            subprocess.Popen(["python", "producer_forecast.py"])
            st.toast("Forecasting Engines Triggered", icon="🚀")
        except Exception as e:
            st.error(f"Error: {e}")
    st.info("System Status: **LIVE**")

st.title("⚡ Energy AI Command Center")

# --- TOP METRICS ROW ---
col1, col2, col3, col4 = st.columns(4)
with col1: metric_price = st.empty()
with col2: metric_wind = st.empty()
with col3: metric_temp = st.empty()
with col4: metric_signal = st.empty()

# ---------------------------------------------------------
# 3. KAFKA SETUP
# ---------------------------------------------------------
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'model.predictions',
        bootstrap_servers=['localhost:9094'],
        auto_offset_reset='latest',
        group_id='dashboard-v6-full',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = get_consumer()

# ---------------------------------------------------------
# 4. DATA BUFFERS
# ---------------------------------------------------------
# "live_buffer" = Short term (Today)
# "forecast_buffer" = Long term (Next 7 Days) for Comparison
live_buffer = [] 
forecast_buffer = []

# ---------------------------------------------------------
# 5. MAIN TABS
# ---------------------------------------------------------
tab_today, tab_battle, tab_raw = st.tabs(["📅 Today's Operations", "⚔️ 7-Day Model Battle", "📝 Raw Data"])

with tab_today:
    st.markdown("### Immediate Market Trend (48h Window)")
    today_chart_placeholder = st.empty()

with tab_battle:
    st.markdown("### Long-Term Strategy (Champion vs Challenger)")
    battle_chart_placeholder = st.empty()

with tab_raw:
    raw_table_placeholder = st.empty()

# ---------------------------------------------------------
# 6. REAL-TIME LOOP
# ---------------------------------------------------------
while True:
    # Non-blocking poll
    msg_pack = consumer.poll(timeout_ms=200)
    
    for tp, messages in msg_pack.items():
        for message in messages:
            payload = message.value
            try:
                # Parse timestamp
                data_time = pd.to_datetime(payload['time'])
                now = datetime.utcnow()
                
                # Extract Values
                price_rf = payload.get('price_rf', 0)
                price_gbt = payload.get('price_gbt', 0)
                wind = payload.get('wind_speed', 0)
                temp = payload.get('temperature', 0)

                # --- LOGIC 1: Update Top Metrics ---
                # We update metrics if the data is "fresh" (close to now)
                # or just use the latest packet received if it's a live stream
                time_diff = (data_time - now).total_seconds()
                
                # If data is within +/- 2 hours of NOW, treat it as "Current Conditions"
                if abs(time_diff) < 7200: 
                    metric_price.metric("Spot Price", f"€ {price_rf:.2f}")
                    metric_wind.metric("Wind Speed", f"{wind:.1f} m/s")
                    metric_temp.metric("Temperature", f"{temp:.1f} °C")
                    
                    # Signal Logic
                    if price_rf < 30:
                        metric_signal.success("RECOMMENDATION: BUY")
                    elif price_rf > 80:
                        metric_signal.error("RECOMMENDATION: SELL")
                    else:
                        metric_signal.info("RECOMMENDATION: HOLD")

                # --- LOGIC 2: Populate "Today" Buffer ---
                # Window: [Now - 12h] to [Now + 24h]
                if -12 < (time_diff / 3600) < 24:
                    live_buffer.append({
                        "Time": data_time,
                        "Price": price_rf,  # Use Champion model for operations
                        "Wind": wind,
                        "Type": "Live"
                    })
                    if len(live_buffer) > 100: live_buffer.pop(0)

                # --- LOGIC 3: Populate "Battle" Buffer ---
                # Window: Future only (> 1h from now)
                if time_diff > 3600:
                    # Append RF Entry
                    forecast_buffer.append({
                        "Time": data_time, "Price": price_rf, "Model": "Random Forest (Champion)"
                    })
                    # Append GBT Entry
                    forecast_buffer.append({
                        "Time": data_time, "Price": price_gbt, "Model": "Gradient Boost (Challenger)"
                    })
                    # Keep buffer manageable
                    if len(forecast_buffer) > 800: forecast_buffer = forecast_buffer[-600:]

            except Exception as e:
                print(f"Error: {e}")
                continue

    # -----------------------------------------------------
    # RENDER SECTION
    # -----------------------------------------------------
    
    # 1. RENDER TODAY'S CHART (Dual Axis: Price + Wind)
    if live_buffer:
        df_today = pd.DataFrame(live_buffer).drop_duplicates(subset=['Time'])
        
        base = alt.Chart(df_today).encode(x=alt.X('Time:T', axis=alt.Axis(format='%H:%M')))
        
        # Wind Area (Background)
        c_wind = base.mark_area(opacity=0.2, color='#00BCD4').encode(
            y=alt.Y('Wind', title='Wind (m/s)'),
            tooltip=['Time', 'Wind']
        )
        # Price Line (Foreground)
        c_price = base.mark_line(color='#FFD700', strokeWidth=3).encode(
            y=alt.Y('Price', title='Price (€)'),
            tooltip=['Time', 'Price']
        )
        
        today_chart = alt.layer(c_wind, c_price).resolve_scale(y='independent').properties(height=350)
        today_chart_placeholder.altair_chart(today_chart, use_container_width=True)

    # 2. RENDER BATTLE CHART (Comparison)
    if forecast_buffer:
        df_battle = pd.DataFrame(forecast_buffer).drop_duplicates()
        
        battle_chart = alt.Chart(df_battle).mark_line().encode(
            x=alt.X('Time:T', axis=alt.Axis(format='%a %d')),
            y=alt.Y('Price', title='Forecast Price (€)', scale=alt.Scale(zero=False)),
            color=alt.Color('Model', scale=alt.Scale(domain=['Random Forest (Champion)', 'Gradient Boost (Challenger)'], range=['#FFD700', '#FF4B4B'])),
            tooltip=['Time', 'Model', 'Price']
        ).properties(height=400).interactive()
        
        battle_chart_placeholder.altair_chart(battle_chart, use_container_width=True)
        
        # Raw Data Table
        if len(forecast_buffer) > 0:
            pivot_df = df_battle.pivot(index="Time", columns="Model", values="Price").sort_values("Time")
            raw_table_placeholder.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)

    # Prevent CPU spike
    time.sleep(0.1)