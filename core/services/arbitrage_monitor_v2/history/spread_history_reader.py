"""
历史数据查询接口

职责：
- 从SQLite数据库查询历史数据
- 支持按代币、时间范围查询
- 优化查询性能（使用索引）
"""

import sqlite3
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import re


class SpreadHistoryReader:
    """历史数据查询器"""
    
    def __init__(self, db_path: str = "data/spread_history/spread_history.db"):
        """
        初始化查询器
        
        Args:
            db_path: SQLite数据库路径
        """
        self.db_path = Path(db_path)
    
    def _normalize_timestamp(self, timestamp_str: str) -> str:
        """
        标准化时间戳格式（转换为ISO格式，兼容数据库格式）
        
        Args:
            timestamp_str: 时间戳字符串（格式：YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DDTHH:MM:SS）
            
        Returns:
            ISO格式时间戳字符串（YYYY-MM-DDTHH:MM:SS）
        """
        if not timestamp_str:
            return timestamp_str
        
        # 如果已经是ISO格式（包含T），直接返回
        if 'T' in timestamp_str:
            return timestamp_str
        
        # 将标准格式转换为ISO格式
        # 格式：YYYY-MM-DD HH:MM:SS -> YYYY-MM-DDTHH:MM:SS
        timestamp_str = timestamp_str.replace(' ', 'T', 1)
        
        return timestamp_str
    
    def query_spread_history(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        min_spread: Optional[float] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        查询历史数据（采样数据）
        
        Args:
            symbol: 代币符号（可选）
            start_date: 开始时间（格式：YYYY-MM-DD HH:MM:SS）
            end_date: 结束时间（格式：YYYY-MM-DD HH:MM:SS）
            min_spread: 最小价差（可选）
            limit: 限制返回条数（可选）
            
        Returns:
            DataFrame包含历史数据
        """
        if not self.db_path.exists():
            return pd.DataFrame()
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT * FROM spread_history_sampled
        WHERE 1=1
        """
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        
        if start_date:
            query += " AND timestamp >= ?"
            # 🔥 标准化时间戳格式（转换为ISO格式，兼容数据库格式）
            params.append(self._normalize_timestamp(start_date))
        
        if end_date:
            query += " AND timestamp <= ?"
            # 🔥 标准化时间戳格式（转换为ISO格式，兼容数据库格式）
            params.append(self._normalize_timestamp(end_date))
        
        if min_spread is not None:
            query += " AND spread_pct >= ?"
            params.append(min_spread)
        
        query += " ORDER BY timestamp"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            print(f"⚠️  查询历史数据失败: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def query_symbol_trend(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        查询指定代币的价差走势（用于图表化）
        
        Args:
            symbol: 代币符号
            start_date: 开始时间（格式：YYYY-MM-DD HH:MM:SS）
            end_date: 结束时间（格式：YYYY-MM-DD HH:MM:SS）
            limit: 限制返回条数（可选）
            
        Returns:
            DataFrame包含价差走势数据
        """
        if not self.db_path.exists():
            return pd.DataFrame()
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            timestamp,
            symbol,
            spread_pct,
            funding_rate_diff_annual,
            price_buy,
            price_sell,
            exchange_buy,
            exchange_sell
        FROM spread_history_sampled
        WHERE symbol = ?
        """
        params = [symbol]
        
        if start_date:
            query += " AND timestamp >= ?"
            # 🔥 标准化时间戳格式（转换为ISO格式，兼容数据库格式）
            params.append(self._normalize_timestamp(start_date))
        
        if end_date:
            query += " AND timestamp <= ?"
            # 🔥 标准化时间戳格式（转换为ISO格式，兼容数据库格式）
            params.append(self._normalize_timestamp(end_date))
        
        query += " ORDER BY timestamp"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            print(f"⚠️  查询代币走势失败: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def query_latest_data(
        self,
        symbol: str,
        minutes: int = 60
    ) -> pd.DataFrame:
        """
        查询最近N分钟的数据
        
        Args:
            symbol: 代币符号
            minutes: 分钟数
            
        Returns:
            DataFrame包含最近的数据
        """
        if not self.db_path.exists():
            return pd.DataFrame()
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT * FROM spread_history_sampled
        WHERE symbol = ? 
        AND timestamp >= datetime('now', '-' || ? || ' minutes')
        ORDER BY timestamp
        """
        
        try:
            df = pd.read_sql_query(query, conn, params=[symbol, minutes])
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            print(f"⚠️  查询最新数据失败: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def get_all_symbols(self) -> List[str]:
        """
        获取所有已记录的代币列表
        
        Returns:
            代币符号列表
        """
        if not self.db_path.exists():
            return []
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT DISTINCT symbol FROM spread_history_sampled
        ORDER BY symbol
        """
        
        try:
            cursor = conn.execute(query)
            symbols = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"⚠️  查询代币列表失败: {e}")
            symbols = []
        finally:
            conn.close()
        
        return symbols
    
    def get_statistics(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取统计信息
        
        Args:
            symbol: 代币符号（可选）
            start_date: 开始时间（可选）
            end_date: 结束时间（可选）
            
        Returns:
            统计信息字典
        """
        df = self.query_spread_history(symbol, start_date, end_date)
        
        if len(df) == 0:
            return {
                'count': 0,
                'mean_spread': 0,
                'max_spread': 0,
                'min_spread': 0,
                'std_spread': 0,
            }
        
        return {
            'count': len(df),
            'mean_spread': float(df['spread_pct'].mean()),
            'max_spread': float(df['spread_pct'].max()),
            'min_spread': float(df['spread_pct'].min()),
            'std_spread': float(df['spread_pct'].std()),
        }

