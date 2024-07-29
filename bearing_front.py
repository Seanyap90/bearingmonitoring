import asyncio
import threading
from queue import Queue as ThreadingQueue
from sseclient import SSEClient
import json
import streamlit as st
import pandas as pd

# Initialize empty DataFrames for each bearing
bearing_names = ['Bearing1_1', 'Bearing3_2', 'Bearing2_2', 'Bearing3_1', 'Bearing2_1', 'Bearing1_2']
data_frames = {name: pd.DataFrame(columns=['Time', 'Hacc', 'Vacc']) for name in bearing_names}
labels = {name: 'N/A' for name in bearing_names}
alerts = pd.DataFrame(columns=['Bearing', 'Time', 'Alert Status'])


# Initialize Streamlit layout
st.set_page_config(layout='wide')
st.title('Live Monitoring of Bearings')
placeholder = st.empty()

def generate_alert_table(alerts_df):
    alerts_df_sorted = alerts_df.iloc[::-1]
    rows = []
    for _, row in alerts_df_sorted.iterrows():
        color = "yellow" if row['Alert Status'] == "verify" else "orange" if row['Alert Status'] == "warning" else "white"
        rows.append(f"<tr style='background-color: {color};'><td>{row['Bearing']}</td><td>{row['Time']}</td><td>{row['Alert Status']}</td></tr>")
    
    table_html = f"""
    <table class='scrollable-table'>
        <thead>
            <tr>
                <th>Bearing</th>
                <th>Time</th>
                <th>Alert Status</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows)}
        </tbody>
    </table>
    """
    
    html_code = f"""
    <div class="table-wrapper">
        {table_html}
    </div>
    <style>
    .table-wrapper {{
        height: 150px; /* Adjust the height as needed */
        overflow-y: auto;
        display: block;
        font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; /* Match Streamlit's font stack */
        border-radius: 4px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
    }}
    .scrollable-table {{
        width: 100%;
        border-collapse: collapse;
    }}
    .scrollable-table th, .scrollable-table td {{
        padding: 8px;
        text-align: left;
        border-bottom: 1px solid #e0e0e0; /* Lighter gray border for consistency */
    }}
    .scrollable-table th {{
        background-color: grey; /* Grey background for header */
        color: #fff; /* White text color for header */
        font-weight: 600; /* Bold header text */
        font-size: 17px; /* Bigger font size for header */
    }}
    .scrollable-table td {{
        color: #333; /* Dark text color for cells */
    }}
    </style>
    """
    
    return html_code

def make_chart(data, bearing_name):
    chart_data = data.set_index('Time')
    st.line_chart(chart_data, height=300)

async def producer(queue, sse_endpoint_url):
    def sse_thread():
        messages = SSEClient(sse_endpoint_url)
        for msg in messages:
            try:
                data = json.loads(msg.data.strip())
                if 'Hacc' in data and 'Vacc' in data:
                    bearing_name = data['Directory']
                    if bearing_name in data_frames:
                        new_row = {'Time': data['Time'], 'Hacc': data['Hacc'], 'Vacc': data['Vacc']}
                        queue.put(('data', bearing_name, new_row))
                        print(f"Added data to queue: {data}")
                elif 'bearing' in data and 'label' in data:
                    bearing_name = data['bearing']
                    if bearing_name in labels:
                        queue.put(('label', bearing_name, data['label']))
                        print(f"Added label to queue: {bearing_name} - {data['label']}")
                elif 'bearing_number' in data and 'alert_status' in data:
                    print(data)
                    bearing_name = data['bearing_number']
                    current_time = data['time']
                    alert_status = data['alert_status']
                    queue.put(('alert', bearing_name, current_time, alert_status))
                    print(f"Added alert to queue: {bearing_name} - {current_time} - {alert_status}")
            except json.JSONDecodeError:
                continue
        print("SSE thread terminated.")

    thread = threading.Thread(target=sse_thread)
    thread.start()
    await asyncio.sleep(0)  # Allow asyncio to run concurrently with the thread
    thread.join() 

async def consumer(queue):
    global alerts
    while True:
        try:
            item = queue.get()
            item_type = item[0]
            bearing_name = item[1]
            data = item[2:]
            print(f'Consumer processed message: {item_type}, {bearing_name}, {data}')
            if item_type == 'data':
                print(data)
                new_row = {'Time': data[0]['Time'], 'Hacc': data[0]['Hacc'], 'Vacc': data[0]['Vacc']}
                data_frames[bearing_name] = pd.concat([data_frames[bearing_name], pd.DataFrame([new_row])])
            elif item_type == 'label':
                print(data)
                labels[bearing_name] = data[0]
            elif item_type == 'alert':
                current_time = data[0]
                alert_status = data[1]
                new_alert = pd.DataFrame([[bearing_name, current_time, alert_status]], columns=['Bearing', 'Time', 'Alert Status'])
                alerts = pd.concat([alerts, new_alert], ignore_index=True)
                print(alerts)
                
            with placeholder.container():
                            
                try:
                    st.markdown("### Alert Log")
                    alert_table_html = generate_alert_table(alerts)
                    st.components.v1.html(alert_table_html, height=200)
                except Exception as e:
                    print(e)
                    pass
                
                for name in bearing_names:
                    cols = st.columns([3, 1])
                    with st.container():
                        with cols[0]:
                            if not data_frames[name].empty:
                                st.markdown(f"### {name}")
                                make_chart(data_frames[name], name)
                        with cols[1]:
                            st.metric(label="Degradation Stage", value=labels[name])
            
            queue.task_done()
        except asyncio.TimeoutError or Exception:
            print("Consumer timed out waiting for message")

async def main():
    queue = ThreadingQueue()
    sse_endpoint_url = "http://192.168.56.102:5001/"

    # Create the producer and consumer tasks
    producer_task = asyncio.create_task(producer(queue, sse_endpoint_url))
    consumer_task = asyncio.create_task(consumer(queue))
    
    # Wait for both tasks to complete
    await asyncio.gather(producer_task, consumer_task)

if __name__ == '__main__':

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass















