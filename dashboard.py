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
    page_title="Energy AI Executive", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Custom Styling for "Dark Mode Professional"
st.markdown("""
<style>
    /* Metric Cards */
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 1px solid #464b5c;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #0E1117;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    /* Recommendation Box */
    .rec-box {
        font-size: 20px;
        font-weight: 700;
        text-align: center;
        padding: 12px;
        border-radius: 6px;
        color: white;
        margin-top: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. SIDEBAR CONTROLS
# ---------------------------------------------------------
with st.sidebar:
    st.header("Control Panel")
    st.markdown("Use this to inject new data into the system.")
    
    # BUTTON: Runs the python script automatically!
    if st.button("Refresh Forecast Data", use_container_width=True):
        try:
            # We use Popen to run it in the background without freezing the dashboard
            subprocess.Popen(["python", "producer_forecast.py"])
            st.toast("Forecast command sent!", icon="✅")
        except Exception as e:
            st.error(f"Failed to run script: {e}")

    st.markdown("---")
    st.info("System status: **ONLINE**")

# ---------------------------------------------------------
# 3. MAIN HEADER
# ---------------------------------------------------------
st.title("Energy market")
st.markdown("Real-time pricing intelligence & meteorological impact analysis.")

# Top KPIs Row
col1, col2, col3, col4 = st.columns(4)
with col1: metric_price = st.empty()
with col2: metric_wind = st.empty()
with col3: metric_temp = st.empty()
with col4: rec_placeholder = st.empty()

# ---------------------------------------------------------
# 4. KAFKA CONNECTION
# ---------------------------------------------------------
@st.cache_resource
def get_consumer():
    # We configure the consumer once here.
    # Group ID ensures we don't conflict with other consumers
    return KafkaConsumer(
        'model.predictions',
        bootstrap_servers=['localhost:9094'],
        auto_offset_reset='latest',
        group_id='dashboard-group-v4', 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = get_consumer()

# Data Buffers
live_data = []
forecast_data = []

# ---------------------------------------------------------
# 5. UI CONTAINERS (TABS)
# ---------------------------------------------------------
tab_live, tab_strat = st.tabs(["Live Operations", "7-Day running"])

with tab_live:
    live_chart = st.empty()
    st.markdown("#### Detailed Live Data")
    live_table = st.empty()

with tab_strat:
    strat_chart = st.empty()
    st.markdown("#### Detailed Forecast Data")
    strat_table = st.empty()

# ---------------------------------------------------------
# 6. STREAMING LOOP (THE FIX)
# ---------------------------------------------------------
# We use a while loop with poll() instead of "for msg in consumer"
# This prevents the "Generator already executing" error on refresh.

placeholder = st.empty()

while True:
    # 1. Poll for messages (wait max 0.5 second)
    # This non-blocking call allows Streamlit to interrupt safely
    msg_pack = consumer.poll(timeout_ms=500) 
    
    for tp, messages in msg_pack.items():
        for message in messages:
            payload = message.value
            
            try:
                data_time = pd.to_datetime(payload['time'])
                now = datetime.utcnow()
                
                # LOGIC: "Future" is anything > 2 hours from now
                is_future = data_time > (now + timedelta(hours=2))
                
                row = {
                    "Time": data_time,
                    "Price (€)": payload['predicted_price'],
                    "Wind (m/s)": payload['wind_speed'],
                    "Temp (°C)": payload.get('temperature', 0)
                }

                # --- ROUTING ---
                if is_future:
                    forecast_data.append(row)
                    # Limit forecast buffer (~1 week)
                    if len(forecast_data) > 300: forecast_data.pop(0)
                else:
                    live_data.append(row)
                    # Limit live buffer
                    if len(live_data) > 60: live_data.pop(0)

                    # Update Metrics
                    metric_price.metric("Current Price", f"€ {row['Price (€)']:.2f}")
                    metric_wind.metric("Wind Speed", f"{row['Wind (m/s)']:.1f} m/s")
                    metric_temp.metric("Temperature", f"{row['Temp (°C)']:.1f} °C")
                    
                    # Recommendation Engine
                    p = row['Price (€)']
                    if p < 30:
                        html = f"<div class='rec-box' style='background:#2E7D32;'>BUY SIGNAL</div>"
                    elif p > 80:
                        html = f"<div class='rec-box' style='background:#C62828;'>SELL SIGNAL</div>"
                    else:
                        html = f"<div class='rec-box' style='background:#455A64;'>HOLD POSITION</div>"
                    rec_placeholder.markdown(html, unsafe_allow_html=True)

            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    # 2. Render UI (Only if we have data)
    
    # RENDER LIVE TAB
    if live_data:
        df_live = pd.DataFrame(live_data)
        base = alt.Chart(df_live).encode(x=alt.X('Time:T', axis=alt.Axis(format='%H:%M:%S', title='TimeUTC')))
        c_wind = base.mark_area(opacity=0.3, color='#00BCD4').encode(y=alt.Y('Wind (m/s)', axis=None))
        c_price = base.mark_line(color='#FFD700', strokeWidth=3).encode(y='Price (€)')
        final_live = alt.layer(c_wind, c_price).resolve_scale(y='independent').properties(height=350)
        
        live_chart.altair_chart(final_live, use_container_width=True)
        live_table.dataframe(df_live.sort_values('Time', ascending=False).style.format({"Price (€)": "{:.2f}", "Wind (m/s)": "{:.1f}"}), use_container_width=True)

    # RENDER STRATEGY TAB
    if forecast_data:
        df_strat = pd.DataFrame(forecast_data).drop_duplicates(subset=['Time']).sort_values('Time')
        c_strat = alt.Chart(df_strat).mark_line(color='#FFA726', point=True).encode(
            x=alt.X('Time:T', axis=alt.Axis(format='%a %d', title='Date', tickCount=7)),
            y=alt.Y('Price (€)', scale=alt.Scale(zero=False)),
            tooltip=['Time', 'Price (€)', 'Wind (m/s)']
        ).properties(height=350).interactive()
        
        strat_chart.altair_chart(c_strat, use_container_width=True)
        strat_table.dataframe(df_strat.style.format({"Price (€)": "{:.2f}"}), use_container_width=True)

    # 3. Small sleep to prevent CPU spike
    time.sleep(0.1)