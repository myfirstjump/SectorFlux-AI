import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go

app = dash.Dash(__name__)

def create_sankey(period):
    # --- 1. 節點定義 (Nodes Definition) ---
    # 分為四個群組，座標區間：x=0(Past), x=0.5(Now), x=1.0(Future)
    # y 軸座標 0 為頂部，1 為底部
    
    # L0 標的 (範例 3 個) + L1 標的 (範例 2 個)
    l0_labels = ["Tech", "Energy", "Financial"]
    l1_labels = ["Semi (SMH)", "Defense (ITA)"]
    hedge_labels = ["Hedge"]
    
    # 組合所有標籤索引 (順序需固定，供 Link 使用)
    all_labels = (
        [f"Past {s}" for s in l0_labels + hedge_labels] +    # 0-3
        [f"Now {s}" for s in l0_labels + hedge_labels] +     # 4-7 (Now-L0)
        [f"Now {s}" for s in l1_labels] +                    # 8-9 (Now-L1 垂直映射區)
        [f"Future {s}" for s in l1_labels]                   # 10-11
    )

    # --- 2. 座標配置 (Manual Positioning) ---
    # 定義每個節點在 Sankey 畫布上的絕對位置
    node_x = [0]*4 + [0.5]*4 + [0.5]*2 + [1]*2
    node_y = [
        0.1, 0.3, 0.5, 0.8,  # Past L0 (左側均分)
        0.1, 0.3, 0.5, 0.8,  # Now L0 (中間上排)
        0.55, 0.7,           # Now L1 (中間下排，產生垂直映射視覺感)
        0.1, 0.6             # Future L1 (右側預測噴發區)
    ]

    # --- 3. 流向定義 (Links) ---
    # 這裡實作：Past -> Now(L0) -> Now(L1) -> Future
    sources = [0, 1, 3, 4, 4, 8, 9] 
    targets = [4, 5, 7, 8, 9, 10, 11]
    values  = [40, 30, 20, 15, 25, 18, 22] # 數值代表 Fund % 或 RS 動能
    
    # 顏色邏輯：與模型設計師的 Confidence Score 連動
    link_colors = [
        "rgba(31, 119, 180, 0.4)", # Past -> Now
        "rgba(31, 119, 180, 0.4)",
        "rgba(128, 128, 128, 0.3)", # Hedge 流向
        "rgba(255, 127, 14, 0.6)",  # L0 -> L1 垂直映射 (Highlight!)
        "rgba(255, 127, 14, 0.6)",
        "rgba(44, 160, 44, 0.7)",   # Future 預測流
        "rgba(44, 160, 44, 0.7)"
    ]

    fig = go.Figure(data=[go.Sankey(
        arrangement = "fixed", # 關鍵：允許自定義 (x, y)
        node = dict(
            pad = 20, thickness = 25,
            line = dict(color = "#2c3e50", width = 1),
            label = all_labels,
            x = node_x, y = node_y,
            color = ["#3498db"]*8 + ["#e67e22"]*2 + ["#2ecc71"]*2
        ),
        link = dict(
            source = sources, target = targets, value = values,
            color = link_colors,
            # Hover 顯示資訊優化
            hovertemplate = '流向量: %{value}<br />來源: %{source.label}<br />目標: %{target.label}<extra></extra>'
        )
    )])

    fig.update_layout(
        title_text=f"SectorFlux-AI: {period} 跨層級資金流向圖",
        font_size=12,
        font_family="Arial",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig

# --- Dash Layout (維持原有結構，注入 V2.0 Figure) ---
app.layout = html.Div([
    html.Div([
        html.H1("SectorFlux-AI", style={'textAlign': 'center', 'color': '#2c3e50'}),
        html.P("L0 產業結構 ➔ L1 題材映射 ➔ 未來 Alpha 預測", style={'textAlign': 'center', 'color': '#7f8c8d'})
    ], className="header"),

    html.Div([
        dcc.RadioItems(
            id='period-selector',
            options=[{'label': '月 (M)', 'value': 'M'},
                     {'label': '季 (Q)', 'value': 'Q'},
                     {'label': '年 (Y)', 'value': 'Y'}],
            value='M',
            inline=True,
            style={'padding': '20px', 'borderRadius': '10px', 'backgroundColor': '#f8f9fa'}
        )
    ], style={'display': 'flex', 'justifyContent': 'center'}),

    html.Div([
        dcc.Graph(id='l0-sankey', config={'displayModeBar': False}, style={'height': '600px'})
    ], className="main-viz"),

    # L2 鎖定介面 (Premium)
    html.Div([
        html.Hr(),
        html.H3("L2: 深度個股群組分析", style={'color': '#95a5a6'}),
        html.Div("🔐 升級至 Premium 以解鎖 IBM Granite-TTM 2660+ 檔標的之動態分群...", 
                 style={'padding': '40px', 'border': '2px dashed #bdc3c7', 'textAlign': 'center', 'color': '#bdc3c7'})
    ])
], style={'padding': '20px', 'maxWidth': '1200px', 'margin': 'auto'})

@app.callback(
    Output('l0-sankey', 'figure'),
    Input('period-selector', 'value')
)
def update_l0_graph(selected_period):
    return create_sankey(selected_period)

if __name__ == '__main__':
    # 針對 Linode 環境的 host/port 設定
    app.run_server(debug=True, host='0.0.0.0', port=8050)