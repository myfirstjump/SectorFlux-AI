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


def _alloc_vec(d: dict) -> np.ndarray:
    """dict → 依 NODE_ORDER 排序、歸一化的權重向量"""
    v = np.array([max(float(d.get(n, 0.0)), 0.0) for n in NODE_ORDER])
    s = v.sum()
    return v / s if s > 0 else v


def _transport(a: np.ndarray, b: np.ndarray):
    """
    配置遷移（transport）：把配置向量 a（左）映射到 b（右），保證
      - 每個來源節點 i 的流出總和 = a[i]   → 左欄節點高度 = a
      - 每個目標節點 j 的流入總和 = b[j]   → 右欄節點高度 = b
    結構：
      carry[i] = min(a[i], b[i])           對角線「留存」（粗）
      殘差 out_res→in_res 按比例重分配       板塊「輪動」（細）
    回傳 links: list of (i, j, value, kind)
    """
    carry   = np.minimum(a, b)
    out_res = a - carry
    in_res  = b - carry
    in_sum  = in_res.sum()
    links = []
    for i in range(len(a)):
        if carry[i] > 1e-9:
            links.append((i, i, float(carry[i]), 'carry'))
    if in_sum > 1e-9:
        for i in range(len(a)):
            if out_res[i] <= 1e-9:
                continue
            for j in range(len(a)):
                if in_res[j] <= 1e-9:
                    continue
                v = out_res[i] * in_res[j] / in_sum
                if v > 1e-4:
                    links.append((i, j, float(v), 'rotate'))
    return links


# ── Sankey ──────────────────────────────────────────────────────
def build_sankey(period: str) -> go.Figure:
    """
    三欄配置遷移 Sankey：過去(LW) → 現在(LW=0) → 未來(模擬)

    設計原則：
    - 節點高度 = 配置佔比（allocation），HEDGE 最大 → 自然落在最底
    - 連線 = allocation transport（對角留存 + 殘差輪動），守恆使節點高度精確
    - arrangement='fixed' + 堆疊式 y 座標：三欄順序一致、HEDGE 永遠最底、互不重疊
    - 連線統一低透明度；hover 凸顯特定節點的資金去向（含近似美元金額）
    """
    data       = _sankey_raw[period]
    now_date   = data['now_date']
    now_alloc  = data['now_alloc']
    past_alloc = data['past_alloc']
    fut_raw    = data.get('fut_alloc')          # 來自 Forecast_NodeFlux（TTM 預測）
    total_mc_b = float(data.get('total_mc_b', 0.0))

    lw_label = '月 M (21D)' if period == 'M' else '季 Q (63D)'
    N = len(NODE_ORDER)

    # ── 三欄配置向量（past / now / future）──────────────────────
    a_past = _alloc_vec(past_alloc)
    a_now  = _alloc_vec(now_alloc)
    if fut_raw:                                  # 真實 TTM 預測
        a_fut = _alloc_vec(fut_raw)
        is_forecast = True
    else:                                        # fallback：隨機模擬佔位
        rng   = np.random.default_rng(seed=42)
        a_fut = a_now * rng.uniform(0.85, 1.15, size=N)
        a_fut = a_fut / a_fut.sum()
        is_forecast = False
    fut_tag = '†' if is_forecast else '*'        # † 預測 / * 模擬

    def pct(vec, i):
        return f"{vec[i]*100:.1f}%"

    past_labels = [f"{NODE_LABELS[NODE_ORDER[i]]} {pct(a_past,i)}"          for i in range(N)]
    now_labels  = [f"{NODE_LABELS[NODE_ORDER[i]]} {pct(a_now, i)}"          for i in range(N)]
    fut_labels  = [f"{NODE_LABELS[NODE_ORDER[i]]} {pct(a_fut, i)}{fut_tag}" for i in range(N)]
    all_labels  = past_labels + now_labels + fut_labels
    _base_colors = [hex_rgba(NODE_COLORS[n], 1.0) for n in NODE_ORDER]
    all_colors   = _base_colors * 3

    # ── 堆疊式 y 座標：slot 高度取三欄最大值，確保任一欄都不重疊 ──
    # 順序 = NODE_ORDER（HEDGE 最後 → 最底）；三欄共用同一 y，視覺對齊
    slot_h = np.array([max(a_past[i], a_now[i], a_fut[i]) for i in range(N)])
    gap    = 0.010
    total  = slot_h.sum() + gap * (N - 1)
    centers, cum = [], 0.0
    for i in range(N):
        centers.append((cum + slot_h[i] / 2) / total)
        cum += slot_h[i] + gap
    centers = [min(max(c, 0.005), 0.995) for c in centers]
    node_x  = [0.02]*N + [0.50]*N + [0.98]*N
    node_y  = centers * 3

    sources, targets, values, link_colors, link_custom = [], [], [], [], []
    LINK_ALPHA = 0.16

    _fut_kind = 'pred' if is_forecast else 'sim'

    def _add(s, t, v, color_node, kind):
        sources.append(s); targets.append(t); values.append(v)
        link_colors.append(hex_rgba(NODE_COLORS[color_node], LINK_ALPHA))
        usd = v * total_mc_b
        src_name = NODE_LABELS[NODE_ORDER[s % N]]
        tgt_name = NODE_LABELS[NODE_ORDER[t % N]]
        tag = {'carry': '留存', 'rotate': '輪動',
               'pred': '預測', 'sim': '模擬'}.get(kind, kind)
        sfx = ' ⚠️' if kind == 'sim' else ''
        link_custom.append(
            f"{src_name} → {tgt_name}<br>{tag}：{v*100:.1f}% 資金"
            + (f"  ≈ ${usd:.1f}B{sfx}" if total_mc_b > 0 else sfx)
        )

    # ── Past → Now（真實配置遷移）────────────────────────────────
    for i, j, v, kind in _transport(a_past, a_now):
        _add(i, N + j, v, NODE_ORDER[i], kind)

    # ── Now → Future（TTM 預測 或 fallback 模擬）─────────────────
    for i, j, v, kind in _transport(a_now, a_fut):
        _add(N + i, 2 * N + j, v, NODE_ORDER[i], _fut_kind)

    fig = go.Figure(data=[go.Sankey(
        arrangement='fixed',
        domain=dict(x=[0, 1], y=[0, 0.92]),   # 上方 8% 保留給欄位標題
        node=dict(
            pad=6, thickness=22,
            line=dict(color='rgba(255,255,255,0.45)', width=1.0),
            label=all_labels,
            x=node_x, y=node_y,
            color=all_colors,
            hovertemplate='%{label}<extra></extra>',
        ),
        link=dict(
            source=sources, target=targets, value=values,
            color=link_colors, customdata=link_custom,
            hovertemplate='%{customdata}<extra></extra>',
        ),
    )])

    # 欄位標題（置於 sankey domain 上方，不與節點重疊）
    fut_head = '未來 † TTM 預測' if is_forecast else '未來 ⚠️ 模擬'
    for xp, txt, col in [
        (0.02, '過去', '#90A4AE'),
        (0.50, '現在', '#ECEFF1'),
        (0.98, fut_head, '#5C8A9E' if is_forecast else '#9E7B3A'),
    ]:
        fig.add_annotation(
            x=xp, y=0.99, xref='paper', yref='paper',
            text=txt, showarrow=False,
            font=dict(color=col, size=14), xanchor='center',
        )

    # 未來區淡色遮罩
    fig.add_shape(type='rect', xref='paper', yref='paper',
                  x0=0.70, x1=1.0, y0=0, y1=0.92,
                  fillcolor='rgba(255,255,255,0.012)',
                  line=dict(color='rgba(255,255,255,0.05)', dash='dot'))

    fig.update_layout(
        title=dict(
            text=f'SectorFlux-AI | L0 板塊配置遷移 [{lw_label}]　定錨日：{now_date}',
            font=dict(size=14, color='#ECEFF1'), x=0.5,
        ),
        paper_bgcolor=BG_DARK, plot_bgcolor=BG_PLOT,
        font=dict(color=TEXT, family='Arial'),
        margin=dict(l=10, r=10, t=70, b=10),
        height=760,
    )
    return fig


# ── Dash App ─────────────────────────────────────────────────────
app = dash.Dash(__name__)
server = app.server  # for gunicorn

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

    # Sankey
    dcc.Graph(
        id='l0-sankey',
        config={'displayModeBar': False},
    ),

    # 注意
    html.Div(
        '† 未來欄為 IBM Granite-TTM-r2（zero-shot）預測之板塊配置；'
        '若無預測資料則退回隨機模擬佔位（標記 *）。',
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
