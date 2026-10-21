"""
图表生成工具

职责：
- 生成Plotly心电图样式图表
- 支持单代币和多代币对比
- 用于数据可视化展示
"""

from typing import List, Optional
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .spread_history_reader import SpreadHistoryReader


class ChartGenerator:
    """图表生成器"""
    
    def __init__(self, db_path: str = "data/spread_history/spread_history.db"):
        """
        初始化图表生成器
        
        Args:
            db_path: SQLite数据库路径
        """
        self.reader = SpreadHistoryReader(db_path)
    
    def create_spread_chart(
        self,
        df: pd.DataFrame,
        symbol: str,
        title: Optional[str] = None
    ) -> go.Figure:
        """
        创建基础价差走势图表
        
        🔥 修改：同一个代币可能有2个方向的价差，需要显示2条线
        
        Args:
            df: 数据DataFrame（包含exchange_buy和exchange_sell字段）
            symbol: 代币符号
            title: 图表标题（可选）
            
        Returns:
            Plotly图表对象
        """
        fig = go.Figure()
        
        # 🔥 修改：根据exchange_buy和exchange_sell分组，显示2条线
        if 'exchange_buy' in df.columns and 'exchange_sell' in df.columns:
            # 获取所有唯一的价差方向组合
            spread_directions = df[['exchange_buy', 'exchange_sell']].drop_duplicates()
            
            # 为每个方向添加一条线
            colors = ['blue', 'green', 'red', 'orange', 'purple']  # 颜色列表
            for idx, (_, row) in enumerate(spread_directions.iterrows()):
                exchange_buy = row['exchange_buy']
                exchange_sell = row['exchange_sell']
                direction_df = df[(df['exchange_buy'] == exchange_buy) & (df['exchange_sell'] == exchange_sell)]
                
                if len(direction_df) > 0:
                    # 按时间排序
                    direction_df = direction_df.sort_values('timestamp')
                    line_name = f"{exchange_buy}买→{exchange_sell}卖"
                    color = colors[idx % len(colors)]
                    
                    fig.add_trace(go.Scatter(
                        x=direction_df['timestamp'],
                        y=direction_df['spread_pct'],
                        mode='lines+markers',
                        name=line_name,
                        line=dict(color=color, width=2),
                        marker=dict(size=4)
                    ))
        else:
            # 如果没有exchange_buy和exchange_sell字段，使用旧逻辑（兼容性）
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['spread_pct'],
                mode='lines+markers',
                name='价差',
                line=dict(color='blue', width=2),
                marker=dict(size=4)
            ))
        
        # 🔥 不再在价差图表中显示资金费率差（已分离为独立图表）
        
        fig.update_layout(
            title=title or f'{symbol} 价差走势图',
            xaxis_title='时间',
            yaxis_title='价差 (%)',
            hovermode='x unified',
            legend=dict(x=0, y=1),
            plot_bgcolor='#1e1e1e',
            paper_bgcolor='#1e1e1e',
            font=dict(color='white'),
            # 🔥 Y轴自适应（价差可能为正数或负数，自动调整）
            yaxis=dict(
                autorange=True,
                automargin=True
            )
        )
        
        return fig
    
    def create_ecg_style_chart(
        self,
        df: pd.DataFrame,
        symbol: str,
        title: Optional[str] = None
    ) -> go.Figure:
        """
        创建心电图样式的价差走势图
        
        效果特点：
        - 连续的折线图，类似心电图波形
        - 深色背景，高对比度线条
        - 网格线辅助读数
        - 高亮显示异常值（高价差）
        - 平滑的曲线过渡
        
        Args:
            df: 数据DataFrame
            symbol: 代币符号
            title: 图表标题（可选）
            
        Returns:
            Plotly图表对象
        """
        if len(df) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color='white')
            )
            fig.update_layout(
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e'
            )
            return fig
        
        fig = go.Figure()
        
        # 🔥 修改：根据exchange_buy和exchange_sell分组，显示2条线
        if 'exchange_buy' in df.columns and 'exchange_sell' in df.columns:
            # 获取所有唯一的价差方向组合
            spread_directions = df[['exchange_buy', 'exchange_sell']].drop_duplicates()
            
            # 为每个方向添加一条线
            colors = ['blue', 'green', 'red', 'orange', 'purple']  # 颜色列表
            for idx, (_, row) in enumerate(spread_directions.iterrows()):
                exchange_buy = row['exchange_buy']
                exchange_sell = row['exchange_sell']
                direction_df = df[(df['exchange_buy'] == exchange_buy) & (df['exchange_sell'] == exchange_sell)].copy()
                
                if len(direction_df) > 0:
                    # 按时间排序
                    direction_df = direction_df.sort_values('timestamp')
                    line_name = f"{exchange_buy}买→{exchange_sell}卖"
                    color = colors[idx % len(colors)]
                    
                    # 计算异常值阈值（价差 > 平均值 + 2倍标准差）
                    mean_spread = direction_df['spread_pct'].mean()
                    std_spread = direction_df['spread_pct'].std()
                    threshold_high = mean_spread + 2 * std_spread
                    threshold_low = mean_spread - 2 * std_spread
                    
                    # 分离正常值和异常值
                    normal_data = direction_df[direction_df['spread_pct'].between(threshold_low, threshold_high)]
                    high_anomaly = direction_df[direction_df['spread_pct'] > threshold_high]
                    low_anomaly = direction_df[direction_df['spread_pct'] < threshold_low]
                    
                    # 添加正常值线
                    if len(normal_data) > 0:
                        fig.add_trace(go.Scatter(
                            x=normal_data['timestamp'],
                            y=normal_data['spread_pct'],
                            mode='lines',
                            name=line_name,
                            line=dict(color=color, width=2),
                            showlegend=True
                        ))
                    
                    # 添加高价差异常值（红色高亮）
                    if len(high_anomaly) > 0:
                        fig.add_trace(go.Scatter(
                            x=high_anomaly['timestamp'],
                            y=high_anomaly['spread_pct'],
                            mode='markers',
                            name=f'{line_name} (高价差)',
                            marker=dict(color='red', size=8, symbol='diamond'),
                            showlegend=True
                        ))
                    
                    # 添加负价差异常值（橙色高亮）
                    if len(low_anomaly) > 0:
                        fig.add_trace(go.Scatter(
                            x=low_anomaly['timestamp'],
                            y=low_anomaly['spread_pct'],
                            mode='markers',
                            name=f'{line_name} (负价差)',
                            marker=dict(color='orange', size=8, symbol='diamond'),
                            showlegend=True
                        ))
        else:
            # 如果没有exchange_buy和exchange_sell字段，使用旧逻辑（兼容性）
            # 计算异常值阈值（价差 > 平均值 + 2倍标准差）
            mean_spread = df['spread_pct'].mean()
            std_spread = df['spread_pct'].std()
            threshold_high = mean_spread + 2 * std_spread
            threshold_low = mean_spread - 2 * std_spread
            
            # 分离正常值和异常值
            normal_data = df[df['spread_pct'].between(threshold_low, threshold_high)]
            high_anomaly = df[df['spread_pct'] > threshold_high]
            low_anomaly = df[df['spread_pct'] < threshold_low]
            
            # 正常值：绿色线条（类似心电图正常波形）
            if len(normal_data) > 0:
                fig.add_trace(go.Scatter(
                    x=normal_data['timestamp'],
                    y=normal_data['spread_pct'],
                    mode='lines',
                    name='正常价差',
                    line=dict(color='#00ff00', width=2, shape='spline'),  # 绿色，平滑曲线
                    hovertemplate='<b>%{fullData.name}</b><br>' +
                                '时间: %{x}<br>' +
                                '价差: %{y:.4f}%<extra></extra>'
                ))
            
            # 高价差异常值：红色高亮（类似心电图异常波形）
            if len(high_anomaly) > 0:
                fig.add_trace(go.Scatter(
                    x=high_anomaly['timestamp'],
                    y=high_anomaly['spread_pct'],
                mode='markers+lines',
                name='高价差（异常）',
                line=dict(color='#ff0000', width=3, shape='spline'),
                marker=dict(size=8, color='#ff0000', symbol='diamond'),
                hovertemplate='<b>⚠️ %{fullData.name}</b><br>' +
                            '时间: %{x}<br>' +
                            '价差: %{y:.4f}%<extra></extra>'
            ))
        
        # 低价差异常值：黄色高亮
        if len(low_anomaly) > 0:
            fig.add_trace(go.Scatter(
                x=low_anomaly['timestamp'],
                y=low_anomaly['spread_pct'],
                mode='markers+lines',
                name='低价差（异常）',
                line=dict(color='#ffff00', width=2, shape='spline'),
                marker=dict(size=6, color='#ffff00', symbol='circle'),
                hovertemplate='<b>⚠️ %{fullData.name}</b><br>' +
                            '时间: %{x}<br>' +
                            '价差: %{y:.4f}%<extra></extra>'
            ))
        
        # 添加参考线（平均值）
        fig.add_hline(
            y=mean_spread,
            line_dash="dash",
            line_color="white",
            annotation_text=f"平均值: {mean_spread:.4f}%",
            annotation_position="right"
        )
        
        # 心电图样式布局
        fig.update_layout(
            title=dict(
                text=title or f'📊 {symbol} 价差走势图（心电图样式）',
                font=dict(size=20, color='white'),
                x=0.5
            ),
            xaxis=dict(
                title=dict(text='时间', font=dict(color='white', size=14)),
                tickfont=dict(color='white'),
                gridcolor='rgba(255, 255, 255, 0.1)',
                gridwidth=1,
                showgrid=True,
                zeroline=False
            ),
            yaxis=dict(
                title=dict(text='价差 (%)', font=dict(color='white', size=14)),
                tickfont=dict(color='white'),
                gridcolor='rgba(255, 255, 255, 0.1)',
                gridwidth=1,
                showgrid=True,
                zeroline=True,
                zerolinecolor='rgba(255, 255, 255, 0.3)',
                zerolinewidth=1,
                # 🔥 自适应Y轴范围（价差通常0.1%-1%，自动调整）
                autorange=True,
                # 添加10%的padding，让图表更易读
                automargin=True
            ),
            plot_bgcolor='#1e1e1e',  # 深色背景
            paper_bgcolor='#1e1e1e',
            font=dict(color='white'),
            hovermode='x unified',
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor='rgba(0, 0, 0, 0.5)',
                bordercolor='white',
                borderwidth=1,
                font=dict(color='white', size=12)
            ),
            height=600,
            width=1200
        )
        
        return fig
    
    def create_multi_symbol_chart(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        title: Optional[str] = None
    ) -> go.Figure:
        """
        创建多个代币的价差走势对比图
        
        Args:
            symbols: 代币符号列表
            start_date: 开始时间
            end_date: 结束时间
            title: 图表标题（可选）
            
        Returns:
            Plotly图表对象
        """
        fig = go.Figure()
        
        colors = ['#00ff00', '#00ffff', '#ff00ff', '#ffff00', '#ff8800', '#ff0088', '#0088ff']
        
        for i, symbol in enumerate(symbols):
            df = self.reader.query_symbol_trend(symbol, start_date, end_date)
            if len(df) > 0:
                fig.add_trace(go.Scatter(
                    x=df['timestamp'],
                    y=df['spread_pct'],
                    mode='lines',
                    name=symbol,
                    line=dict(color=colors[i % len(colors)], width=2)
                ))
        
        fig.update_layout(
            title=title or '多个代币价差走势对比',
            xaxis_title='时间',
            yaxis_title='价差 (%)',
            hovermode='x unified',
            plot_bgcolor='#1e1e1e',
            paper_bgcolor='#1e1e1e',
            font=dict(color='white'),
            legend=dict(
                bgcolor='rgba(0, 0, 0, 0.5)',
                bordercolor='white',
                borderwidth=1,
                font=dict(color='white', size=12)
            ),
            height=600,
            width=1200
        )
        
        return fig
    
    def create_ecg_multi_channel_chart(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        title: Optional[str] = None
    ) -> go.Figure:
        """
        创建多通道心电图样式图表（类似多导联心电图）
        
        效果：多个代币的价差走势并排显示，类似心电图的多导联显示
        
        Args:
            symbols: 代币符号列表
            start_date: 开始时间
            end_date: 结束时间
            title: 图表标题（可选）
            
        Returns:
            Plotly图表对象
        """
        num_symbols = len(symbols)
        fig = make_subplots(
            rows=num_symbols,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            subplot_titles=symbols
        )
        
        colors = ['#00ff00', '#00ffff', '#ff00ff', '#ffff00', '#ff8800']
        
        for i, symbol in enumerate(symbols):
            df = self.reader.query_symbol_trend(symbol, start_date, end_date)
            if len(df) > 0:
                # 计算偏移量（让每个通道分开显示）
                offset = i * 0.5  # 每个通道偏移0.5%
                y_values = df['spread_pct'] + offset
                
                fig.add_trace(
                    go.Scatter(
                        x=df['timestamp'],
                        y=y_values,
                        mode='lines',
                        name=symbol,
                        line=dict(color=colors[i % len(colors)], width=2, shape='spline'),
                        showlegend=False
                    ),
                    row=i+1,
                    col=1
                )
                
                # 添加参考线
                mean_val = df['spread_pct'].mean() + offset
                fig.add_hline(
                    y=mean_val,
                    line_dash="dash",
                    line_color="rgba(255, 255, 255, 0.3)",
                    line_width=1,
                    row=i+1,
                    col=1
                )
        
        # 心电图样式布局
        fig.update_layout(
            title=dict(
                text=title or '📊 多代币价差走势图（多通道心电图样式）',
                font=dict(size=20, color='white'),
                x=0.5
            ),
            plot_bgcolor='#1e1e1e',
            paper_bgcolor='#1e1e1e',
            font=dict(color='white'),
            height=200 * num_symbols,
            width=1200
        )
        
        # 更新所有子图的样式
        for i in range(num_symbols):
            fig.update_xaxes(
                gridcolor='rgba(255, 255, 255, 0.1)',
                gridwidth=1,
                showgrid=True,
                zeroline=False,
                row=i+1,
                col=1
            )
            fig.update_yaxes(
                gridcolor='rgba(255, 255, 255, 0.1)',
                gridwidth=1,
                showgrid=True,
                zeroline=True,
                zerolinecolor='rgba(255, 255, 255, 0.3)',
                row=i+1,
                col=1
            )
        
        return fig
    
    def create_symbol_chart_from_db(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        minutes: Optional[int] = None,
        style: str = "ecg"
    ) -> go.Figure:
        """
        从数据库查询数据并创建图表
        
        Args:
            symbol: 代币符号
            start_date: 开始时间（可选）
            end_date: 结束时间（可选）
            minutes: 最近N分钟（可选，优先级高于start_date/end_date）
            style: 图表样式（"ecg"或"normal"）
            
        Returns:
            Plotly图表对象
        """
        if minutes:
            df = self.reader.query_latest_data(symbol, minutes)
        else:
            df = self.reader.query_symbol_trend(symbol, start_date, end_date)
        
        if style == "ecg":
            return self.create_ecg_style_chart(df, symbol)
        else:
            return self.create_spread_chart(df, symbol)
    
    def create_funding_rate_chart(
        self,
        df: pd.DataFrame,
        symbol: str,
        title: Optional[str] = None,
        style: str = "ecg"
    ) -> go.Figure:
        """
        创建资金费率差走势图表（独立图表）
        
        Args:
            df: 数据DataFrame
            symbol: 代币符号
            title: 图表标题（可选）
            style: 图表样式（"ecg"或"normal"）
            
        Returns:
            Plotly图表对象
        """
        if len(df) == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color='white')
            )
            fig.update_layout(
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e'
            )
            return fig
        
        # 检查是否有资金费率差数据
        if 'funding_rate_diff_annual' not in df.columns or df['funding_rate_diff_annual'].notna().sum() == 0:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无资金费率差数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color='white')
            )
            fig.update_layout(
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e'
            )
            return fig
        
        fig = go.Figure()
        
        # 🔥 从8小时费率差计算年化值（不使用数据库中可能错误的年化值）
        # 因为历史数据可能使用了错误的计算公式（365*24而不是1095）
        if 'funding_rate_diff' in df.columns:
            # 从8小时费率差计算年化值：8小时费率差 × 1095 × 100（转换为百分比）
            # funding_rate_diff是小数形式（如0.0001表示0.01%），年化后需要乘以100显示为百分比
            # 🔥 资金费率差应该永远为正数（绝对值差值）
            df['funding_rate_diff_annual_calculated'] = abs(df['funding_rate_diff']) * 1095 * 100
            valid_data = df[df['funding_rate_diff_annual_calculated'].notna()]
        else:
            # 如果没有8小时费率差字段，尝试使用存储的年化值
            # 注意：历史数据可能存储的是小数形式或百分比形式，需要判断
            valid_data = df[df['funding_rate_diff_annual'].notna()].copy()
            if len(valid_data) > 0:
                # 如果存储的年化值看起来是小数形式（绝对值小于1），需要乘以100
                # 如果已经是百分比形式（绝对值大于1），直接使用
                # 🔥 资金费率差应该永远为正数（绝对值差值）
                sample_value = valid_data['funding_rate_diff_annual'].iloc[0]
                if abs(sample_value) < 1:
                    # 小数形式，需要乘以100转换为百分比，并使用绝对值
                    valid_data['funding_rate_diff_annual_calculated'] = abs(valid_data['funding_rate_diff_annual']) * 100
                else:
                    # 已经是百分比形式，使用绝对值确保为正数
                    valid_data['funding_rate_diff_annual_calculated'] = abs(valid_data['funding_rate_diff_annual'])
        
        if len(valid_data) == 0:
            fig.add_annotation(
                text="暂无有效资金费率差数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color='white')
            )
            fig.update_layout(
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e'
            )
            return fig
        
        if style == "ecg":
            # 心电图样式
            # 计算异常值阈值（使用正确计算的年化值）
            mean_funding = valid_data['funding_rate_diff_annual_calculated'].mean()
            std_funding = valid_data['funding_rate_diff_annual_calculated'].std()
            threshold_high = mean_funding + 2 * std_funding
            threshold_low = mean_funding - 2 * std_funding
            
            # 分离正常值和异常值
            normal_data = valid_data[valid_data['funding_rate_diff_annual_calculated'].between(threshold_low, threshold_high)]
            high_anomaly = valid_data[valid_data['funding_rate_diff_annual_calculated'] > threshold_high]
            low_anomaly = valid_data[valid_data['funding_rate_diff_annual_calculated'] < threshold_low]
            
            # 正常值：青色线条
            if len(normal_data) > 0:
                fig.add_trace(go.Scatter(
                    x=normal_data['timestamp'],
                    y=normal_data['funding_rate_diff_annual_calculated'],
                    mode='lines',
                    name='正常资金费率差',
                    line=dict(color='#00ffff', width=2, shape='spline'),
                    hovertemplate='<b>%{fullData.name}</b><br>' +
                                '时间: %{x}<br>' +
                                '资金费率差: %{y:.4f}%<extra></extra>'
                ))
            
            # 高异常值：红色高亮
            if len(high_anomaly) > 0:
                fig.add_trace(go.Scatter(
                    x=high_anomaly['timestamp'],
                    y=high_anomaly['funding_rate_diff_annual_calculated'],
                    mode='markers+lines',
                    name='高资金费率差（异常）',
                    line=dict(color='#ff0000', width=3, shape='spline'),
                    marker=dict(size=8, color='#ff0000', symbol='diamond'),
                    hovertemplate='<b>⚠️ %{fullData.name}</b><br>' +
                                '时间: %{x}<br>' +
                                '资金费率差: %{y:.4f}%<extra></extra>'
                ))
            
            # 低异常值：黄色高亮
            if len(low_anomaly) > 0:
                fig.add_trace(go.Scatter(
                    x=low_anomaly['timestamp'],
                    y=low_anomaly['funding_rate_diff_annual_calculated'],
                    mode='markers+lines',
                    name='低资金费率差（异常）',
                    line=dict(color='#ffff00', width=2, shape='spline'),
                    marker=dict(size=6, color='#ffff00', symbol='circle'),
                    hovertemplate='<b>⚠️ %{fullData.name}</b><br>' +
                                '时间: %{x}<br>' +
                                '资金费率差: %{y:.4f}%<extra></extra>'
                ))
            
            # 添加参考线（平均值）
            fig.add_hline(
                y=mean_funding,
                line_dash="dash",
                line_color="white",
                annotation_text=f"平均值: {mean_funding:.4f}%",
                annotation_position="right"
            )
            
            # 心电图样式布局
            fig.update_layout(
                title=dict(
                    text=title or f'📊 {symbol} 资金费率差走势图（心电图样式）',
                    font=dict(size=20, color='white'),
                    x=0.5
                ),
                xaxis=dict(
                    title=dict(text='时间', font=dict(color='white', size=14)),
                    tickfont=dict(color='white'),
                    gridcolor='rgba(255, 255, 255, 0.1)',
                    gridwidth=1,
                    showgrid=True,
                    zeroline=False
                ),
                yaxis=dict(
                    title=dict(text='资金费率差（年化%）', font=dict(color='white', size=14)),
                    tickfont=dict(color='white'),
                    gridcolor='rgba(255, 255, 255, 0.1)',
                    gridwidth=1,
                    showgrid=True,
                    zeroline=True,
                    zerolinecolor='rgba(255, 255, 255, 0.3)',
                    zerolinewidth=1,
                    # 🔥 自适应Y轴范围（资金费率差可能-50%到+几百%，自动调整）
                    autorange=True,
                    # 确保0线可见（如果数据跨越正负值）
                    rangemode='normal',
                    # 添加10%的padding，让图表更易读
                    automargin=True
                ),
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e',
                font=dict(color='white'),
                hovermode='x unified',
                legend=dict(
                    x=0.02,
                    y=0.98,
                    bgcolor='rgba(0, 0, 0, 0.5)',
                    bordercolor='white',
                    borderwidth=1,
                    font=dict(color='white', size=12)
                ),
                height=600,
                width=1200
            )
        else:
            # 普通样式
            fig.add_trace(go.Scatter(
                x=valid_data['timestamp'],
                y=valid_data['funding_rate_diff_annual'],
                mode='lines+markers',
                name='资金费率差（年化）',
                line=dict(color='orange', width=2),
                marker=dict(size=4)
            ))
            
            fig.update_layout(
                title=title or f'{symbol} 资金费率差走势图',
                xaxis_title='时间',
                yaxis_title='资金费率差（年化%）',
                hovermode='x unified',
                legend=dict(x=0, y=1),
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e',
                font=dict(color='white'),
                # 🔥 Y轴自适应（资金费率差可能-50%到+几百%，自动调整）
                yaxis=dict(
                    autorange=True,
                    rangemode='normal',
                    automargin=True
                )
            )
        
        return fig

