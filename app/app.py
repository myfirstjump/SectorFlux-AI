import os, json
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# ── 常數 ────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

NODE_ORDER = ['XLK','XLF','XLV','XLE','XLI','XLY','XLP','XLU','XLB','XLRE','XLC','HEDGE']
STACK_ORDER = ['XLRE','XLB','XLP','XLU','XLY','XLC','XLI','XLE','XLV','XLF','XLK','HEDGE']

NODE_LABELS = {
    'XLK':'Technology','XLF':'Financials','XLV':'Health Care',
    'XLE':'Energy','XLI':'Industrials','XLY':'Consumer Discr',
    'XLP':'Consumer Staples','XLU':'Utilities','XLB':'Materials',
    'XLRE':'Real Estate','XLC':'Communication','HEDGE':'HEDGE (BIL+SHV+TLT+GLD)',
}
NODE_COLORS = {
    'XLRE':'#AD1457','XLB':'#558B2F','XLP':'#F9A825',
    'XLU':'#00838F','XLY':'#6A1B9A','XLC':'#283593',
    'XLI':'#4E342E','XLE':'#E65100','XLV':'#C62828',
    'XLF':'#2E7D32','XLK':'#1565C0','HEDGE':'#FFA000',
}

BG_DARK = '#16213E'
BG_PLOT = '#1A1A2E'
TEXT    = '#B0BEC5'


def hex_rgba(h: str, a: float) -> str:
    h = h.lstrip('#')
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f'rgba({r},{g},{b},{a})'


# ── 資料載入（啟動時一次性讀取靜態 JSON）──────────────────────────
def _load(fname):
    with open(os.path.join(DATA_DIR, fname), encoding='utf-8') as f:
        return json.load(f)

_alloc_raw  = _load('alloc_timeseries.json')   # [{date, node, weight}]
_spy_raw    = _load('spy_prices.json')         # [{date, spy_close}]
_sankey_raw = {
    'M': _load('sankey_M.json'),
    'Q': _load('sankey_Q.json'),
}

# 預處理時序圖
_alloc_df = pd.DataFrame(_alloc_raw)
_alloc_pivot = (
    _alloc_df.pivot(index='date', columns='node', values='weight')
    .fillna(0)
    .sort_index()
)
_alloc_pivot.index = pd.to_datetime(_alloc_pivot.index)

_spy_df = pd.DataFrame(_spy_raw).set_index('date')
_spy_df.index = pd.to_datetime(_spy_df.index)


# ── 時序圖 ──────────────────────────────────────────────────────
def build_timeseries() -> go.Figure:
    pivot = _alloc_pivot
    spy   = _spy_df['spy_close']

    fig = go.Figure()
    for node in STACK_ORDER:
        if node not in pivot.columns:
            continue
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot[node] * 100,
            name=NODE_LABELS.get(node, node),
            mode='lines', stackgroup='alloc',
            fillcolor=hex_rgba(NODE_COLORS[node], 0.85 if node != 'HEDGE' else 1.0),
            line=dict(color=NODE_COLORS[node], width=0.5 if node != 'HEDGE' else 1.5),
            hovertemplate=f'<b>{NODE_LABELS.get(node,node)}</b><br>%{{y:.1f}}%<extra></extra>',
            yaxis='y',
        ))

    spy_aligned = spy.reindex(pivot.index, method='ffill')
    fig.add_trace(go.Scatter(
        x=spy_aligned.index, y=spy_aligned.values,
        name='SPY Price', mode='lines',
        line=dict(color='#ECEFF1', width=2), opacity=0.9,
        hovertemplate='<b>SPY</b> $%{y:.2f}<extra></extra>',
        yaxis='y2',
    ))

    fig.add_shape(type='line', xref='x', yref='paper',
                  x0='2025-12-05', x1='2025-12-05', y0=0, y1=1,
                  line=dict(color='rgba(255,255,255,0.3)', width=1, dash='dash'))
    fig.add_annotation(x='2025-12-05', y=1.02, xref='x', yref='paper',
                       text='L0 Split', showarrow=False,
                       font=dict(color='#90A4AE', size=10), xanchor='left')

    fig.update_layout(
        title=dict(text='L0 板塊配置水位 × SPY 走勢（日線，LW=0 當日快照）',
                   font=dict(size=14, color='#ECEFF1'), x=0.5),
        paper_bgcolor=BG_DARK, plot_bgcolor=BG_PLOT,
        font=dict(color=TEXT, family='Arial'),
        hovermode='x unified',
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label='1Y', step='year', stepmode='backward'),
                    dict(count=2, label='2Y', step='year', stepmode='backward'),
                    dict(count=3, label='3Y', step='year', stepmode='backward'),
                    dict(step='all', label='ALL'),
                ],
                bgcolor='#0F3460', activecolor='#E94560', font=dict(color='#ECEFF1'),
            ),
            showgrid=True, gridcolor='rgba(255,255,255,0.05)', color=TEXT,
        ),
        yaxis=dict(
            title='Allocation Weight (%)', range=[0, 100],
            showgrid=True, gridcolor='rgba(255,255,255,0.07)',
            tickformat='.0f', ticksuffix='%', color=TEXT,
        ),
        yaxis2=dict(
            title='SPY Close ($)', overlaying='y', side='right',
            showgrid=False, tickformat=',.0f', tickprefix='$', color='#ECEFF1',
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0.4)', bordercolor='rgba(255,255,255,0.1)',
            borderwidth=1, font=dict(size=10),
            x=1.08, y=1, traceorder='reversed',
        ),
        margin=dict(l=60, r=180, t=60, b=40),
        height=520,
    )
    return fig


# ── Sankey ──────────────────────────────────────────────────────
def build_sankey(period: str) -> go.Figure:
    data       = _sankey_raw[period]
    now_date   = data['now_date']
    now_alloc  = data['now_alloc']   # {node: weight}
    past_alloc = data['past_alloc']  # {node: weight}
    flux_rows  = data['flux']        # [{src, tgt, amount_b}]

    lw_label = '月 M (21D)' if period == 'M' else '季 Q (63D)'
    N = len(NODE_ORDER)
    idx = {node: i for i, node in enumerate(NODE_ORDER)}

    # ── 節點 label（3 列：Past / Now / Future）──────────────────
    def make_labels(alloc: dict, prefix: str) -> list[str]:
        return [
            f"{prefix}{NODE_LABELS.get(n, n)}<br><span style='font-size:10px'>{alloc.get(n,0)*100:.1f}%</span>"
            for n in NODE_ORDER
        ]

    # future allocation = random perturbation of now (placeholder)
    rng = np.random.default_rng(seed=42)
    raw_fut = np.array([now_alloc.get(n, 1/N) for n in NODE_ORDER])
    raw_fut = raw_fut * rng.uniform(0.85, 1.15, size=N)
    raw_fut = raw_fut / raw_fut.sum()
    fut_alloc = {NODE_ORDER[i]: float(raw_fut[i]) for i in range(N)}

    all_labels = make_labels(past_alloc, '') + make_labels(now_alloc, '') + make_labels(fut_alloc, '')
    all_colors = [NODE_COLORS[n] for n in NODE_ORDER] * 3

    # ── 節點 Y 位置（依 now_alloc weight 由大到小排，視覺上大塊在上）──
    sorted_nodes = sorted(NODE_ORDER, key=lambda n: now_alloc.get(n, 0), reverse=True)
    y_map = {node: 0.05 + (i / (N - 1)) * 0.88 for i, node in enumerate(sorted_nodes)}

    node_x = [0.02] * N + [0.50] * N + [0.98] * N
    node_y = [y_map[n] for n in NODE_ORDER] * 3

    # ── 連線：Past → Now（Fact_NodeFlux 真實數據）──────────────
    sources, targets, values, link_colors = [], [], [], []

    if flux_rows:
        # 取 top-50 筆避免過擁擠
        top_flux = sorted(flux_rows, key=lambda r: r['amount_b'], reverse=True)[:50]
        for row in top_flux:
            s, t, amt = row['src'], row['tgt'], float(row['amount_b'])
            if s not in idx or t not in idx or amt <= 0:
                continue
            sources.append(idx[s])
            targets.append(N + idx[t])
            values.append(amt)
            link_colors.append(hex_rgba(NODE_COLORS.get(s, '#888'), 0.50))

    # ── 連線：Now → Future（隨機佔位，標記為模擬）──────────────
    for i, src_node in enumerate(NODE_ORDER):
        w = now_alloc.get(src_node, 0)
        if w < 0.008:
            continue
        n_t = rng.integers(2, 4)
        tgt_idxs  = rng.choice(N, size=n_t, replace=False)
        wts = rng.dirichlet(np.ones(n_t))
        for j, ti in enumerate(tgt_idxs):
            amt = float(w * wts[j] * 800)
            if amt < 1.0:
                continue
            sources.append(N + i)
            targets.append(2 * N + int(ti))
            values.append(amt)
            link_colors.append(hex_rgba(NODE_COLORS.get(src_node, '#888'), 0.15))

    fig = go.Figure(data=[go.Sankey(
        arrangement='fixed',
        node=dict(
            pad=8, thickness=20,
            line=dict(color='rgba(255,255,255,0.12)', width=0.5),
            label=all_labels,
            x=node_x, y=node_y,
            color=all_colors,
            hovertemplate='%{label}<extra></extra>',
        ),
        link=dict(
            source=sources, target=targets, value=values, color=link_colors,
            hovertemplate='%{source.label} → %{target.label}<br>$%{value:.2f}B<extra></extra>',
        ),
    )])

    # 列標題
    for xp, txt, col in [
        (0.02, '過去', '#90A4AE'),
        (0.50, '現在', '#ECEFF1'),
        (0.98, '未來 ⚠️模擬', '#546E7A'),
    ]:
        fig.add_annotation(x=xp, y=1.04, xref='paper', yref='paper',
                           text=txt, showarrow=False,
                           font=dict(color=col, size=13, family='Arial'),
                           xanchor='center')

    # 未來區域淡色背景
    fig.add_shape(type='rect', xref='paper', yref='paper',
                  x0=0.76, x1=1.0, y0=0, y1=1,
                  fillcolor='rgba(255,255,255,0.015)',
                  line=dict(color='rgba(255,255,255,0.06)', dash='dot'))

    fig.update_layout(
        title=dict(
            text=f'SectorFlux-AI | L0 資金流向 Sankey [{lw_label}]  定錨日：{now_date}',
            font=dict(size=14, color='#ECEFF1'), x=0.5,
        ),
        paper_bgcolor=BG_DARK, plot_bgcolor=BG_PLOT,
        font=dict(color=TEXT, family='Arial'),
        margin=dict(l=10, r=10, t=80, b=10),
        height=740,
    )
    return fig


# ── Dash App ─────────────────────────────────────────────────────
app = dash.Dash(__name__)
server = app.server  # for gunicorn

_ts_fig = build_timeseries()

app.layout = html.Div([

    html.Div([
        html.H1('SectorFlux-AI',
                style={'textAlign':'center','color':'#ECEFF1','margin':0,'fontSize':'24px'}),
        html.P('L0 板塊資金流向分析系統',
               style={'textAlign':'center','color':'#90A4AE','margin':'4px 0 0','fontSize':'13px'}),
    ], style={'padding':'20px 0 12px','borderBottom':'1px solid rgba(255,255,255,0.08)'}),

    # Period selector
    html.Div([
        html.Label('Sankey 觀察週期：', style={'color':'#90A4AE','marginRight':'10px','fontSize':'13px'}),
        dcc.RadioItems(
            id='period-selector',
            options=[
                {'label': '月 M（21 交易日）', 'value': 'M'},
                {'label': '季 Q（63 交易日）', 'value': 'Q'},
            ],
            value='M', inline=True,
            style={'color':'#ECEFF1'},
            labelStyle={'marginRight':'24px','cursor':'pointer','fontSize':'13px'},
            inputStyle={'marginRight':'6px'},
        ),
    ], style={
        'display':'flex','alignItems':'center','justifyContent':'center',
        'padding':'12px 20px','margin':'12px 0',
        'backgroundColor':'rgba(255,255,255,0.04)','borderRadius':'8px',
    }),

    # 時序圖
    dcc.Graph(
        id='alloc-timeseries',
        figure=_ts_fig,
        config={'displayModeBar': True, 'scrollZoom': True},
        style={'marginBottom': '12px'},
    ),

    # Sankey
    dcc.Graph(
        id='l0-sankey',
        config={'displayModeBar': False},
    ),

    # 注意
    html.Div(
        '⚠️ 未來流向為隨機模擬佔位符（Forecast_NodeFlux 尚無 IBM Granite-TTM 推論結果）',
        style={
            'textAlign':'center','color':'#546E7A','fontSize':'11px',
            'padding':'10px 0','borderTop':'1px solid rgba(255,255,255,0.05)','marginTop':'8px',
        }
    ),

], style={
    'backgroundColor': BG_DARK,
    'minHeight': '100vh',
    'padding': '0 20px 24px',
    'fontFamily': 'Arial, sans-serif',
})


@app.callback(Output('l0-sankey', 'figure'), Input('period-selector', 'value'))
def update_sankey(period):
    return build_sankey(period)


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
