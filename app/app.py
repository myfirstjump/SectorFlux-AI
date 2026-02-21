# app/app.py
import dash
from dash import dcc, html
import plotly.graph_objects as go

app = dash.Dash(__name__)
server = app.server

# 假設的產業清單
sectors = ['半導體', '金融', '醫療', '能源', '科技', '消費', '工業', '原物料']
# 預設等分角度與寬度
theta = [i * (360 / len(sectors)) for i in range(len(sectors))]
width = [360 / len(sectors) - 2] * len(sectors) # 減去 2 留出間隙

def create_polar_ring(base_radius, r_value, colors, show_text=False):
    """
    建立獨立的極座標環
    base_radius: 內徑起始點
    r_value: 環的厚度
    """
    fig = go.Figure()
    
    fig.add_trace(go.Barpolar(
        r=[r_value] * len(sectors),
        theta=theta,
        width=width,
        base=[base_radius] * len(sectors),
        marker_color=colors,
        marker_line_color="#222222",
        marker_line_width=2,
        hovertext=sectors,
        hoverinfo="text"
    ))

    # 加入產業標籤 (僅在外環顯示以保持整潔)
    if show_text:
        fig.add_trace(go.Scatterpolar(
            r=[base_radius + r_value/2] * len(sectors),
            theta=theta,
            mode='text',
            text=sectors,
            textfont=dict(color='white', size=14),
            hoverinfo='none'
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=False, range=[0, 10]), # 鎖定比例尺，確保三層完美對齊
            angularaxis=dict(visible=False),
            bgcolor='rgba(0,0,0,0)'
        ),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=0, b=0, l=0, r=0)
    )
    return fig

# 設定三環顏色與參數
colors_outer = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
colors_mid = ['rgba(31, 119, 180, 0.7)', 'rgba(255, 127, 14, 0.7)', 'rgba(44, 160, 44, 0.7)', 'rgba(214, 39, 40, 0.7)', 'rgba(148, 103, 189, 0.7)', 'rgba(140, 86, 75, 0.7)', 'rgba(227, 119, 194, 0.7)', 'rgba(127, 127, 127, 0.7)']
colors_inner = ['rgba(31, 119, 180, 0.4)', 'rgba(255, 127, 14, 0.4)', 'rgba(44, 160, 44, 0.4)', 'rgba(214, 39, 40, 0.4)', 'rgba(148, 103, 189, 0.4)', 'rgba(140, 86, 75, 0.4)', 'rgba(227, 119, 194, 0.4)', 'rgba(127, 127, 127, 0.4)']

fig_outer = create_polar_ring(base_radius=7, r_value=2, colors=colors_outer, show_text=True)
fig_mid = create_polar_ring(base_radius=4.5, r_value=2, colors=colors_mid)
fig_inner = create_polar_ring(base_radius=2, r_value=2, colors=colors_inner)

app.layout = html.Div([
    html.H1("SectorFlux-AI: 產業動能輪動", style={'textAlign': 'center', 'color': 'white', 'paddingTop': '20px'}),
    
    html.Div([
        # 外環 (歷史)：緩慢逆時針 
        html.Div(dcc.Graph(figure=fig_outer, config={'displayModeBar': False}), className='ring outer-circle'),
        # 中環 (現在)：正常速度順時針 
        html.Div(dcc.Graph(figure=fig_mid, config={'displayModeBar': False}), className='ring middle-circle'),
        # 內環 (未來)：快速順時針 (TimesFM 動能) 
        html.Div(dcc.Graph(figure=fig_inner, config={'displayModeBar': False}), className='ring inner-circle'),
        
        # 中間定位點 / 瞄準線基礎
        html.Div(className='center-anchor')
    ], className='concentric-container')
    
], style={'backgroundColor': '#111111', 'minHeight': '100vh', 'fontFamily': 'sans-serif'})

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)