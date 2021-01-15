##옥수수가격 대시보드 만들기######################

import matplotlib.pyplot as plt
import pandas as pd
import pandas_datareader as pdr
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

external_style = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def graph_corn():
    plt.rcParams["figure.figsize"] = (14,4)
    plt.rcParams['axes.grid'] = True


    #국제 옥수수가격
    df_corn = pdr.DataReader('PMAIZMTUSDQ', 'fred', start='2015-01-01')
    df_exchange = pdr.DataReader('DEXKOUS', 'fred', start='2015-01-01')
    # df_zinc = pdr.DataReader('PZINCUSDM', 'fred', start='2000-01-01')
    # print('row count:', len(df_corn))
    # print('row count:', len(df_exchange))
    df_all = pd.concat([df_corn, df_exchange], axis=1)
    df_all = df_all.rename({'PMAIZMTUSDQ': 'CORN', 'DEXKOUS':'EXCHANGE'}, axis='columns')
    df_all = df_all.fillna(method='ffill')#결측값채우기
    # df_all = df_all.fillna(method='ffill')
    df_all['price'] = df_all['CORN'] * df_all['EXCHANGE'] /1000

    g =px.line(df_all)

    # ax=df_all.plot(kind='line', y='CORN', color='Blue')
    #
    # ax2=df_all.plot(kind='line',y='EXCHANGE', secondary_y=True, color='Red', ax=ax)
    # ax3=df_all.plot(kind='line',y='price', color='Green', ax=ax)
    # ax.set_ylabel('CORN')
    # ax2.set_ylabel('EXCHANGE')
    # # ax3.set_ylabel('price')
    # plt.tight_layout()

    return g
app = dash.Dash(__name__, external_stylesheets=external_style)
app.layout = html.Div(children=[
    html.H1(children='옥수수가격'),
    html.Br(),
    dcc.Graph(figure=graph_corn())
])

if __name__ == "__main__":
    app.run_server(debug=True)