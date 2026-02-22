import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go

app = dash.Dash(__name__)

# 模擬 L0 Sankey 資料結構 (等待後端 API 餵入)
def create_sankey(period):
    # 這裡將介接 tsf_modules 產出的資料
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15, thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = ["Past Tech", "Past Energy", "Now Tech", "Now Energy", "Future Tech"], # 示意
          color = "blue"
        ),
        link = dict(
          source = [0, 1, 0, 2, 3], # indices correspond to labels
          target = [2, 3, 3, 4, 4],
          value = [8, 4, 2, 8, 4],
          color = "rgba(100, 149, 237, 0.5)" # 這裡會綁定模型設計師的 confidence_score
      ))])
    fig.update_layout(title_text=f"L0: GICS 流向預測 ({period})", font_size=10, paper_bgcolor='rgba(0,0,0,0)')
    return fig

app.layout = html.Div([
    html.H1("SectorFlux-AI", className="header-title"),
    
    # 全局時間維度控制器
    html.Div([
        dcc.RadioItems(
            id='period-selector',
            options=[{'label': '月 (Month)', 'value': 'M'}, 
                     {'label': '季 (Quarter)', 'value': 'Q'}, 
                     {'label': '年 (Year)', 'value': 'Y'}],
            value='M',
            inline=True
        )
    ], className="control-panel"),

    # L0 區塊
    html.Div([
        dcc.Graph(id='l0-sankey', config={'displayModeBar': False})
    ], className="l0-container"),

    # L1 區塊
    html.Div([
        html.H3("L1: 候選 ETF 與題材 (請點擊上方板塊)"),
        html.Div(id='l1-content')
    ], className="l1-container"),

    # L2 區塊 (Premium)
    html.Div([
        html.H3("L2: 資金動態分群 (Premium 專屬)"),
        html.Div("解鎖以查看 IBM Granite-TTM 信心度連動分析...", className="premium-lock")
    ], className="l2-container")

])

# Callback 處理 M/Q/Y 切換
@app.callback(
    Output('l0-sankey', 'figure'),
    Input('period-selector', 'value')
)
def update_l0_graph(selected_period):
    return create_sankey(selected_period)

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)