"""
vw_Allocation_History — L0 板塊配置水位 × SPY 走勢
資料來源：Fact_NodeAllocation（DB 端預算好），SPY 收盤價
記憶體策略：啟動時載入 ~1MB 聚合資料，無原始明細 Cache，符合 1.5GB 容器限制
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from py_module.config import Configuration
from py_module.database import DatabaseManipulation

load_dotenv()

# ── 節點顏色（HEDGE 金色最顯眼，其餘 GICS 標準色）─────────────────
NODE_COLORS = {
    'XLRE': '#AD1457',  # 深粉 — Real Estate
    'XLB':  '#558B2F',  # 橄欖綠 — Materials
    'XLP':  '#F9A825',  # 琥珀 — Consumer Staples
    'XLU':  '#00838F',  # 深青 — Utilities
    'XLY':  '#6A1B9A',  # 深紫 — Consumer Discr
    'XLC':  '#283593',  # 靛藍 — Communication
    'XLI':  '#4E342E',  # 棕 — Industrials
    'XLE':  '#E65100',  # 深橙 — Energy
    'XLV':  '#C62828',  # 深紅 — Health Care
    'XLF':  '#2E7D32',  # 深綠 — Financials
    'XLK':  '#1565C0',  # 深藍 — Technology
    'HEDGE':'#FFA000',  # 金 — HEDGE（BIL+SHV+TLT+GLD）
}

# 堆疊順序：小比重在底、HEDGE 最頂（視覺最突出）
STACK_ORDER = [
    'XLRE','XLB','XLP','XLU','XLY',
    'XLC','XLI','XLE','XLV','XLF','XLK',
    'HEDGE',
]

SECTOR_NAMES = {
    'XLK':'Technology','XLF':'Financials','XLV':'Health Care',
    'XLE':'Energy','XLI':'Industrials','XLY':'Consumer Discr',
    'XLP':'Consumer Staples','XLU':'Utilities','XLB':'Materials',
    'XLRE':'Real Estate','XLC':'Communication','HEDGE':'HEDGE (BIL+SHV+TLT+GLD)',
}


def load_data(lookback_window: int = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """從 DB 載入 Allocation + SPY 資料（一次性，約 1MB）

    lookback_window=0（預設）：當日實際配置快照，適合時序圖。
    lookback_window=21/63：對應 Sankey past-side 的配置，通常由 app.py 使用。
    """
    config = Configuration()
    db     = DatabaseManipulation(config)

    df_alloc = db.execute_query("""
        SELECT CONVERT(VARCHAR,Date,23) AS Date, Node_ID, Weight
        FROM Fact_NodeAllocation WITH (NOLOCK)
        WHERE Lookback_Window = :lw
        ORDER BY Date, Node_ID
    """, params={"lw": lookback_window})

    df_spy = db.execute_query("""
        SELECT CONVERT(VARCHAR,Date,23) AS Date, [Close] AS SPY_Close
        FROM Fact_DailyPrice WITH (NOLOCK)
        WHERE Symbol = 'SPY'
          AND Date >= '2020-01-01'
        ORDER BY Date
    """)

    return df_alloc, df_spy


def build_figure(df_alloc: pd.DataFrame, df_spy: pd.DataFrame,
                 lookback_window: int = 21,
                 sectors_only: bool = False) -> go.Figure:
    """組裝雙 Y 軸圖：堆疊面積（配置比例）+ SPY 折線

    sectors_only=True：去除 HEDGE，對 11 個板塊重新歸一化，突顯板塊輪動。
    """

    # ── 寬表轉換 ──────────────────────────────────────────────
    pivot = df_alloc.pivot(index='Date', columns='Node_ID', values='Weight').fillna(0)
    pivot.index = pd.to_datetime(pivot.index)
    pivot = pivot.sort_index()

    if sectors_only:
        # 去除 HEDGE，對剩餘板塊重新歸一化到 100%
        if 'HEDGE' in pivot.columns:
            pivot = pivot.drop(columns=['HEDGE'])
        row_sum = pivot.sum(axis=1).replace(0, float('nan'))
        pivot = pivot.div(row_sum, axis=0).fillna(0)

    spy = df_spy.set_index(pd.to_datetime(df_spy['Date']))['SPY_Close']

    lw_label = 'M (21D)' if lookback_window == 21 else 'Q (63D)'
    mode_label = ' [板塊輪動]' if sectors_only else ''

    fig = go.Figure()

    # ── 堆疊面積 traces（底→頂）─────────────────────────────
    for node in STACK_ORDER:
        if node not in pivot.columns:
            continue
        if sectors_only and node == 'HEDGE':
            continue
        pct = pivot[node] * 100
        opacity = 0.85 if node != 'HEDGE' else 1.0

        fig.add_trace(go.Scatter(
            x=pivot.index, y=pct,
            name=SECTOR_NAMES.get(node, node),
            mode='lines',
            stackgroup='allocation',
            fillcolor=NODE_COLORS[node].replace(')', f',{opacity})').replace('rgb', 'rgba') if 'rgb' in NODE_COLORS[node] else NODE_COLORS[node],
            line=dict(color=NODE_COLORS[node], width=0.5 if node != 'HEDGE' else 1.5),
            hovertemplate=f'<b>{SECTOR_NAMES.get(node, node)}</b><br>%{{y:.1f}}%<extra></extra>',
            yaxis='y',
        ))

    # ── SPY 折線（右軸）─────────────────────────────────────
    spy_aligned = spy.reindex(pivot.index, method='ffill')
    fig.add_trace(go.Scatter(
        x=spy_aligned.index, y=spy_aligned.values,
        name='SPY Price',
        mode='lines',
        line=dict(color='#ECEFF1', width=2, dash='solid'),
        opacity=0.9,
        hovertemplate='<b>SPY</b> $%{y:.2f}<extra></extra>',
        yaxis='y2',
    ))

    # ── 拆股事件標記線 ──────────────────────────────────────
    fig.add_shape(
        type='line', xref='x', yref='paper',
        x0='2025-12-05', x1='2025-12-05', y0=0, y1=1,
        line=dict(color='rgba(255,255,255,0.35)', width=1, dash='dash'),
    )
    fig.add_annotation(
        x='2025-12-05', y=1.02, xref='x', yref='paper',
        text='L0 Split', showarrow=False,
        font=dict(color='#90A4AE', size=10),
        xanchor='left',
    )

    # ── Layout ──────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=f'SectorFlux-AI | L0 板塊配置水位 × SPY 走勢  [{lw_label}]{mode_label}',
            font=dict(size=16, color='#ECEFF1'),
            x=0.5,
        ),
        plot_bgcolor='#1A1A2E',
        paper_bgcolor='#16213E',
        font=dict(color='#B0BEC5', family='Arial'),
        hovermode='x unified',

        xaxis=dict(
            rangeslider=dict(visible=False),
            rangeselector=dict(
                buttons=[
                    dict(count=1,  label='1Y',  step='year',  stepmode='backward'),
                    dict(count=2,  label='2Y',  step='year',  stepmode='backward'),
                    dict(count=3,  label='3Y',  step='year',  stepmode='backward'),
                    dict(step='all', label='ALL'),
                ],
                bgcolor='#0F3460', activecolor='#E94560',
                font=dict(color='#ECEFF1'),
            ),
            showgrid=True, gridcolor='rgba(255,255,255,0.05)',
            color='#90A4AE',
        ),

        yaxis=dict(
            title='Allocation Weight (%)',
            title_font=dict(color='#90A4AE'),
            range=[0, 100],
            showgrid=True, gridcolor='rgba(255,255,255,0.07)',
            tickformat='.0f', ticksuffix='%',
            color='#90A4AE',
        ),
        yaxis2=dict(
            title='SPY Close ($)',
            title_font=dict(color='#ECEFF1'),
            overlaying='y', side='right',
            showgrid=False,
            tickformat=',.0f', tickprefix='$',
            color='#ECEFF1',
        ),

        legend=dict(
            bgcolor='rgba(0,0,0,0.4)', bordercolor='rgba(255,255,255,0.1)',
            borderwidth=1, font=dict(size=11),
            orientation='v', x=1.08, y=1,
            traceorder='reversed',  # HEDGE 在 legend 最頂
        ),
        margin=dict(l=60, r=160, t=80, b=60),
        height=620,
    )

    return fig


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--lw', type=int, default=0, choices=[0, 21, 63],
                        help='Lookback Window (0=當日快照, 21=M, 63=Q)')
    parser.add_argument('--out', type=str, default='/tmp/allocation_view.html')
    parser.add_argument('--sectors-only', action='store_true',
                        help='去除 HEDGE，對 11 板塊重新歸一化，突顯輪動')
    args = parser.parse_args()

    print(f'Loading data (LW={args.lw})...')
    df_alloc, df_spy = load_data(args.lw)
    print(f'  Allocation: {len(df_alloc)} rows  SPY: {len(df_spy)} rows')

    fig = build_figure(df_alloc, df_spy, args.lw, sectors_only=args.sectors_only)
    fig.write_html(args.out, include_plotlyjs='cdn', full_html=True)
    print(f'Saved → {args.out}')
