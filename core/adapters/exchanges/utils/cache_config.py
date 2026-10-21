"""
交易所适配器统一缓存配置

定义所有交易所适配器使用的统一缓存TTL配置
"""

# 🔥 统一缓存TTL配置（秒）
CACHE_TTL_CONFIG = {
    'balance': 60,      # 余额缓存60秒
    'position': 30,     # 持仓缓存30秒
    'orderbook': 5,      # 订单簿缓存5秒
    'ticker': 10,        # Ticker缓存10秒
    'market_info': 300,  # 市场信息缓存5分钟（静态配置）
    'user_stats': 180,   # 用户统计缓存180秒（Lighter专用）
}

# 🔥 余额自动刷新间隔（秒）
BALANCE_REFRESH_INTERVAL = 15.0  # 从3秒改为15秒，减少API调用频率

def get_cache_ttl(cache_type: str) -> int:
    """
    获取指定类型的缓存TTL
    
    Args:
        cache_type: 缓存类型（balance, position, orderbook, ticker等）
    
    Returns:
        缓存TTL（秒）
    """
    return CACHE_TTL_CONFIG.get(cache_type, 60)  # 默认60秒

def get_balance_refresh_interval() -> float:
    """
    获取余额自动刷新间隔
    
    Returns:
        刷新间隔（秒）
    """
    return BALANCE_REFRESH_INTERVAL

