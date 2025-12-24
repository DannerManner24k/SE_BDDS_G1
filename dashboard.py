import streamlit as st
import json
import pandas as pd
import time
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import altair as alt

# ---------------------------------------------------------
# 1. PROFESSIONAL CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(page_title="Energy Trading Cockpit", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for the "Institutional" look
st.markdown("""
<style>
    /* Darker background for metric cards */
    div[data-testid="metric-container"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 8px;
    }
    /* Clean fonts */
    h1, h2, h3 { font-family: 'Helvetica Neue', sans-serif; font-weight: 400; }
    h1 { color: #FFFFFF; font-size: 2.5rem; margin-bottom: 0px; }
    h3 { color: #AAAAAA; font-size: 1.2rem; margin-top: 20px; }
    
    /* Trading Signal Box */
    .signal-box {
        font-size: 24px; 
        font-weight: bold; 
        text-align: center; 
        padding: 10px; 
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

st.title("Energy Trading Cockpit")
st.markdown("---")

# ---------------------------------------------------------
# 2. LAYOUT: METRICS ROW
# ---------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    metric_price = st.empty()
with col2:
    metric_wind = st.empty()
with col3:
    metric_temp = st.empty()
with col4:
    # We use a placeholder for the custom HTML signal box
    signal_placeholder = st.empty()

# ---------------------------------------------------------
# 3. LAYOUT: CHARTS
# ---------------------------------------------------------
st.markdown("### Live Market Pulse (Last 60 Minutes)")
live_chart_placeholder = st.empty()

st.markdown("### 7-Day Market Strategy")
forecast_chart_placeholder = st.empty()

# ---------------------------------------------------------
# 4. KAFKA SETUP
# ---------------------------------------------------------
consumer = KafkaConsumer(
    'model.predictions',
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Data Buffers
live_data = []
forecast_data = []

print("🖥️ Dashboard V3 (Pro) listening...")

# ---------------------------------------------------------
# 5. LIVE LOOP
# ---------------------------------------------------------
for message in consumer:
    payload = message.value
    
    try:
        data_time = pd.to_datetime(payload['time'])
        current_system_time = datetime.utcnow()
        
        # Router: Live vs Forecast
        is_forecast = data_time > (current_system_time + timedelta(hours=2))
        
        record = {
            "time": data_time,
            "price": payload['predicted_price'],
            "wind": payload['wind_speed']
        }

        # --- LOGIC: Forecast Buffer ---
        if is_forecast:
            forecast_data.append(record)
            if len(forecast_data) > 300: 
                forecast_data = forecast_data[-168:] # Keep ~7 days
        
        # --- LOGIC: Live Buffer ---
        else:
            live_data.append(record)
            if len(live_data) > 100: 
                live_data.pop(0)

            # Update Metrics (Only on Live ticks)
            price = payload['predicted_price']
            wind = payload['wind_speed']
            temp = payload['temperature']
            
            metric_price.metric("Spot Price", f"€ {price:.2f}")
            metric_wind.metric("Wind Speed", f"{wind:.1f} m/s")
            metric_temp.metric("Temperature", f"{temp:.1f} °C")
            
            # Smart Recommendation Logic
            if price < 20: 
                signal_html = f"<div class='signal-box' style='background-color: #1B5E20; color: #4CAF50;'>BUY</div>"
            elif price > 80:
                signal_html = f"<div class='signal-box' style='background-color: #B71C1C; color: #FFCDD2;'>SELL</div>"
            else:
                signal_html = f"<div class='signal-box' style='background-color: #263238; color: #B0BEC5;'>HOLD</div>"
            
            signal_placeholder.markdown(f"**Our Recommendation**{signal_html}", unsafe_allow_html=True)

        # -----------------------------------------------------
        # 6. RENDER: LIVE CHART (Detailed)
        # -----------------------------------------------------
        if live_data:
            df_live = pd.DataFrame(live_data)
            
            base = alt.Chart(df_live).encode(x=alt.X('time:T', axis=alt.Axis(format='%H:%M:%S', title=None)))
            
            # Wind = Teal Area (Background)
            area_wind = base.mark_area(opacity=0.2, color='#00BCD4').encode(
                y=alt.Y('wind', axis=None),
                tooltip=['time', 'wind']
            )
            
            # Price = White/Gold Line (Foreground)
            line_price = base.mark_line(color='#FFD700', strokeWidth=2).encode(
                y=alt.Y('price', title='Price (€)', scale=alt.Scale(zero=False)),
                tooltip=['time', 'price']
            )
            
            chart = alt.layer(area_wind, line_price).resolve_scale(y='independent').properties(height=350)
            live_chart_placeholder.altair_chart(chart, use_container_width=True)

        # -----------------------------------------------------
        # 7. RENDER: FORECAST CHART (Cleaned Up)
        # -----------------------------------------------------
        if forecast_data and len(forecast_data) > 10:
            df_forecast = pd.DataFrame(forecast_data).drop_duplicates(subset=['time']).sort_values('time')
            
            forecast_chart = alt.Chart(df_forecast).mark_line(
                color='#FFD700', 
                interpolate='monotone' # Smooths the line
            ).encode(
                # CLEAN X-AXIS: Shows Day Name + Day Number (e.g. "Fri 26")
                x=alt.X('time:T', axis=alt.Axis(format='%a %d', tickCount=7, title=None, labelColor='#888')),
                y=alt.Y('price', title='Forecast (€)', scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip('time', format='%A %d %H:00'), 
                    alt.Tooltip('price', format='.2f'), 
                    alt.Tooltip('wind', format='.1f')
                ]
            ).properties(height=250)
            
            forecast_chart_placeholder.altair_chart(forecast_chart, use_container_width=True)

    except Exception as e:
        pass