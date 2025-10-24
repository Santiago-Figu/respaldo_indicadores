from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
# import pytz

def format_datetime_with_timezone(
    dt: datetime,
    timezone_str: str = "America/Mexico_City",
    format_str: str = "%d/%m/%Y %H:%M",
    assume_utc_if_naive: bool = True
) -> str:
    """
    Formatea un datetime en la zona horaria deseada.
    
    Args:
        dt: Objeto datetime (naive o con timezone)
        timezone_str: Zona horaria (ej. "America/Mexico_City")
        format_str: Formato de strftime (ej. "%d/%m/%Y %H:%M")
        assume_utc_if_naive: Si es True, asume UTC cuando dt no tiene timezone
    
    Returns:
        String formateado en la zona horaria deseada
    """
    if dt.tzinfo is None and assume_utc_if_naive:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    
    target_tz = ZoneInfo(timezone_str)
    localized_dt = dt.astimezone(target_tz)
    
    return localized_dt.strftime(format_str)