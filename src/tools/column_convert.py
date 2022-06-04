import pandas as pd
from datetime import datetime


def datetime_to_save(dt_sr: pd.Series) -> pd.Series:
  return dt_sr.map(lambda el: el.strftime("%Y-%m-%d %H:$M:$S"))


def datetime_from_save(dt_sr: pd.Series) -> pd.Series:
  return dt_sr.map(lambda el: datetime.strptime(el, "%Y-%m-%d %H:$M:$S"))
