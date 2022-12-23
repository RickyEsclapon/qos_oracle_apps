import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

# Automatically refresh app every 5 minutes - stops after 25 times
count = st_autorefresh(interval=300000, limit=25, key="fizzbuzzcounter")

@st.cache
def get_subgraph_info(total_rows):
    # Initialize an empty list to store the results
    results = []
    # Set the number of requests to make
    num_requests = total_rows // 1000
    # Make the GraphQL requests
    for i in range(num_requests):
        # Set the GraphQL query
        query = '''
        query {
          subgraphDeployments(first: 1000, skip: ''' + str(i*1000) + ''') {
            ipfsHash
            originalName
            signalledTokens
          }
        }
        '''
        # Send the GraphQL request
        r = requests.post("https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet", json={'query': query})
        # Load result into json
        json_data = json.loads(r.text)
        # st.write(json_data)
        # Convert json into a dataframe
        df = pd.DataFrame(json_data['data']['subgraphDeployments'])
        # Add the dataframe to the list
        results.append(df)
    # Union the dataframes into a single dataframe
    df = pd.concat(results)
    # Return the results
    return df
# pull subgraphs info
subgraphs_info = get_subgraph_info(2000)


# Markdown title
st.title('Gateway QoS Oracle by Indexer')

# Get list of indexers
query = str('''{
  indexers(first:1000){
    id
  }
}''')
# set endpoint url
url = 'https://api.thegraph.com/subgraphs/name/juanmardefago/gateway-qos-oracle'
r = requests.post(url, json={'query': query})
# load result into json
json_data = json.loads(r.text)
# convert json into a dataframe
indexers = pd.DataFrame(json_data['data']['indexers'])
# show list of indexers
# st.dataframe(indexers)

# find index of datanexus as default example for selection
default_indexer = int(indexers["id"].str.find("0x87eba079059b75504c734820d6cf828476754b83")[lambda x : x != -1].index[0])
# choose indexer
indexer_sel = st.selectbox('Select Indexer',indexers['id'], index = default_indexer)

nrows = st.slider('How many rows of data do you want to pull? One observation per subgraph every 5 minutes', 1000, 50000, 10000, 1000)

# initialize text
t = st.empty()

@st.cache
def pull_data(nrows):
  # Initialize an empty list to store the dataframes
  df_list = []
  # Initialize the minimum end_epoch value to a very large number
  min_epoch = 999999999999999999
  # Get data for the indexer (30k rows)
  for i in range(int(nrows/1000)):
      # Display the updated text using the st.cache function
      t.markdown(str("#### Now pulled " + str(i*1+1) + ",000 rows from subgraph"))
      # Get data for the indexer
      query = str('''{
        indexerDataPoints(orderBy: end_epoch, orderDirection: desc, where:{indexer: "'''+indexer_sel+'''", end_epoch_lte: '''+str(min_epoch)+'''}, first: 1000){
          end_epoch
          indexer_url
          indexer_wallet
          subgraph_deployment_ipfs_hash
          avg_indexer_blocks_behind
          avg_indexer_latency_ms
          avg_query_fee
          max_indexer_blocks_behind
          max_indexer_latency_ms
          max_query_fee
          num_indexer_200_responses
          proportion_indexer_200_responses
          query_count
          start_epoch
          stdev_indexer_latency_ms
          total_query_fees
          }
      }''')
      # Set endpoint url
      url = 'https://api.thegraph.com/subgraphs/name/juanmardefago/gateway-qos-oracle'
      r = requests.post(url, json={'query': query})
      # Load result into json
      json_data = json.loads(r.text)
      # st.write(json_data)
      # Convert json into a dataframe
      df = pd.DataFrame(json_data['data']['indexerDataPoints'])
      # Convert unix timestamp to date
      df['date'] = pd.to_datetime(df['end_epoch'],unit='s')
      # Update the minimum end_epoch value
      min_epoch = min(min_epoch, df['end_epoch'].astype(int).min())
      # Add the dataframe to the list
      df_list.append(df)
  return df_list
# pull data
df_list = pull_data(nrows)
# Union the dataframes into a single dataframe
df = pd.concat(df_list)

# Join subgraphs_info into new data
df = pd.merge(left=df, right=subgraphs_info, left_on='subgraph_deployment_ipfs_hash', right_on='ipfsHash', how='inner')
# create column which takes subgraph name, but uses ipfs hash when it doesn't exist
df['subgraph'] = df['originalName'].where(df['originalName'].notnull(), df['subgraph_deployment_ipfs_hash'])

# show data (only if data is less than 15k rows)
if df.shape[0] < 15000:
  st.write("5 Minute Interval Data")
  st.dataframe(df.style.hide_index())

# Download data button
@st.cache
def convert_df(df):
    # Caches the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')
# convert
csv = convert_df(df)
# show button
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name='indexer_data.csv',
    mime='text/csv',
)

# Column to visualize
col_viz = st.selectbox('Which column do you want to visualize?', 
                          ('query_count','avg_indexer_blocks_behind','avg_indexer_latency_ms',
                          'avg_query_fee','max_indexer_blocks_behind','max_indexer_latency_ms',
                          'max_query_fee','num_indexer_200_responses','proportion_indexer_200_responses',
                          'query_count',#'stdev_indexer_latency_ms',
                          'total_query_fees'))
# time interval
time_interval = st.selectbox('Choose a time interval', ('1 hour', '5 minutes'))
                          
# Convert column to numeric
df[col_viz] = pd.to_numeric(df[col_viz])

# 5 minute interval data
if time_interval == '5 minutes':
  st.write("5 minute interval data of `" + col_viz + "` for indexer `" + indexer_sel + "`")
  # Visualize data (5 min interval)
  fig = px.line(
      df,
      x="date",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph"
  )
  # fig.update_layout(showlegend=False)
  st.plotly_chart(fig, theme="streamlit", use_container_width=True)

# 1 hour interval data
if time_interval == '1 hour':
  st.write("Hourly data of `" + col_viz + "` for indexer `" + indexer_sel + "`")
  
  def truncate_date(date):
    date = date.replace(minute=0, second=0)
    return date
  
  # Convert to hourly data - truncate minutes
  df['hour'] = df['date'].apply(truncate_date)
  # exclude min and max hours (may be incomplete data)
  min_hour = df['hour'].min()
  max_hour = df['hour'].max()
  data_viz = df[df['hour'] != min_hour]
  data_viz = data_viz[data_viz['hour'] != max_hour]
  # apply proper transformation based on variable of choice
  if col_viz == 'query_count':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).query_count.sum().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  elif col_viz == 'total_query_fees':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).total_query_fees.sum().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  elif col_viz == 'num_indexer_200_responses':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).num_indexer_200_responses.sum().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  elif col_viz == 'max_indexer_blocks_behind':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).max_indexer_blocks_behind.max().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  elif col_viz == 'max_indexer_latency':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).max_indexer_latency.max().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  elif col_viz == 'max_query_fee':
    # visualize
    fig = px.line(
      data_viz.groupby([data_viz['hour'], 'subgraph']).max_query_fee.max().reset_index(name=col_viz),
      x="hour",
      y=col_viz,
      # size="pop",
      color="subgraph",
      hover_name="subgraph")
    # fig.update_layout(showlegend=False)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  else:
    st.write('still adding')


