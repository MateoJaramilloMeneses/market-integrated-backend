# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import requests
import yfinance as yf

def tweet_id_to_datetime(tweet_id: int) -> Optional[datetime]:
    """
    Convierte un ID de tweet (snowflake) en un datetime UTC aproximado.
    Fórmula basada en el esquema de IDs de Twitter/X.
    """
    try:
        twitter_epoch_ms = 1288834974657  # 2010-11-04T01:42:54.657Z
        timestamp_ms = (tweet_id >> 22) + twitter_epoch_ms
        return datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    except Exception:
        return None


SERPAPI_API_KEY = "510c17bb85d95cd2111329ad2aa6b14e2004fca83a26c2eca6db17c685178568"


app = FastAPI(
    title="Market Integrated Backend",
    description="API para métricas de acciones, noticias (GDELT) y tweets.",
    version="1.0.0",
)


# ---------- MODELOS DE RESPUESTA ----------

class StockResponse(BaseModel):
    symbol: str
    date: str
    close: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    var_day: Optional[float] = None
    var_week: Optional[float] = None
    var_month: Optional[float] = None


class NewsItem(BaseModel):
    title: str
    url: str
    source: Optional[str] = None
    language: Optional[str] = None
    datetime: Optional[str] = None
    snippet: Optional[str] = None


class NewsResponse(BaseModel):
    keyword: str
    date: str
    articles: List[NewsItem]


class TweetItem(BaseModel):
    user: str
    text: str
    created_at: str
    url: Optional[str] = None


class TweetsResponse(BaseModel):
    keyword: str
    date: str
    tweets: List[TweetItem]


# ---------- ENDPOINT: /stocks ----------

@app.get("/stocks", response_model=StockResponse)
def get_stock(
    symbol: str = "EC",
    date: str = "2024-01-15",
):
    """
    Devuelve métricas de una acción para una fecha específica,
    además de variaciones aproximadas diario, semanal y mensual.
    - symbol: ticker (ajústalo al formato correcto según Yahoo Finance)
    - date: YYYY-MM-DD
    """

    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")

    # Para calcular variaciones necesitamos ventana histórica
    start = target_date - timedelta(days=40)
    end = target_date + timedelta(days=1)

    data = yf.download(symbol, start=start.isoformat(), end=end.isoformat())

    if data.empty:
        raise HTTPException(status_code=404, detail="No se encontraron datos para ese símbolo/fecha.")

    # Intentar localizar el día objetivo (puede no ser día hábil)
    # Buscamos la fila más cercana <= date
    data = data.sort_index()
    target_idx = None
    for idx in reversed(data.index):
        if idx.date() <= target_date:
            target_idx = idx
            break

    if target_idx is None:
        raise HTTPException(status_code=404, detail="No hay datos previos a esa fecha.")

    # Índices para variaciones
    def get_close_at(days_delta: int):
        ref_date = target_date - timedelta(days=days_delta)
        # buscamos la fila con fecha <= ref_date
        ref_idx = None
        for idx in reversed(data.index):
            if idx.date() <= ref_date:
                ref_idx = idx
                break
        if ref_idx is None:
            return None
        return float(data.loc[ref_idx]["Close"])

    close_today = float(data.loc[target_idx]["Close"])
    open_today = float(data.loc[target_idx]["Open"])
    high_today = float(data.loc[target_idx]["High"])
    low_today = float(data.loc[target_idx]["Low"])
    volume_today = float(data.loc[target_idx]["Volume"])

    # Variaciones
    close_yesterday = get_close_at(1)
    close_week = get_close_at(7)
    close_month = get_close_at(30)

    def var_rel(curr, past):
        if curr is None or past is None or past == 0:
            return None
        return (curr - past) / past

    var_day = var_rel(close_today, close_yesterday)
    var_week = var_rel(close_today, close_week)
    var_month = var_rel(close_today, close_month)

    return StockResponse(
        symbol=symbol,
        date=target_date.isoformat(),
        close=close_today,
        open=open_today,
        high=high_today,
        low=low_today,
        volume=volume_today,
        var_day=var_day,
        var_week=var_week,
        var_month=var_month,
    )


# ---------- ENDPOINT: /news (GDELT) ----------

@app.get("/news", response_model=NewsResponse)
def get_news(
    keyword: str = "Ecopetrol",
    date: str = "2024-01-15",
    maxrecords: int = 50,
):
    """
    Envuelve la API de GDELT (v2/doc) para traer noticias de un día sobre una palabra clave.
    - keyword: término de búsqueda (Ecopetrol, por ejemplo)
    - date: YYYY-MM-DD
    """

    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Usa YYYY-MM-DD.")

    start_dt = target_date.replace(hour=0, minute=0, second=0)
    end_dt = target_date.replace(hour=23, minute=59, second=59)

    # Formato GDELT: YYYYMMDDHHMMSS
    start_str = start_dt.strftime("%Y%m%d%H%M%S")
    end_str = end_dt.strftime("%Y%m%d%H%M%S")

    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": keyword,
        "mode": "artlist",
        "maxrecords": maxrecords,
        "format": "json",
        "startdatetime": start_str,
        "enddatetime": end_str,
    }

    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Error consultando GDELT.")

    data = resp.json()
    raw_articles = data.get("articles", [])

    articles: List[NewsItem] = []

    for a in raw_articles:
        articles.append(
            NewsItem(
                title=a.get("title", ""),
                url=a.get("url", ""),
                source=a.get("sourceCountry", None),
                language=a.get("language", None),
                datetime=a.get("seendate", None),
                snippet=a.get("title", None),
            )
        )

    return NewsResponse(
        keyword=keyword,
        date=date,
        articles=articles,
    )

# ---------- ENDPOINT: /tweets (Google + organic_results aproximados) ----------

@app.get("/tweets", response_model=TweetsResponse)
def get_tweets(
    keyword: str = "Ecopetrol",
    date: str = "2024-01-15",
    max_results: int = 20,
):
    """
    Aproxima 'tweets' usando resultados orgánicos de Google que apuntan a X/Twitter.
    NO son tweets individuales completos, sino enlaces/snippets relacionados.

    - keyword: palabra a buscar (Ecopetrol, por ejemplo).
    - date: se devuelve tal cual (no podemos inferir fecha real del tweet).
    - max_results: máximo de resultados orgánicos a analizar.
    """

    if not SERPAPI_API_KEY:
        raise HTTPException(status_code=500, detail="Falta configurar SERPAPI_API_KEY en el servidor.")

    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": f"{keyword} site:x.com OR site:twitter.com",
        "api_key": SERPAPI_API_KEY,
        "num": max_results,
    }

    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Error SerpAPI {resp.status_code}: {resp.text[:200] if resp.text else 'Sin respuesta'}",
        )

    data = resp.json()

    organic = data.get("organic_results", []) or []

    tweets: List[TweetItem] = []

    for r in organic:
        link = r.get("link", "") or ""
        source = r.get("source", "") or ""
        snippet = r.get("snippet", "") or ""
        title = r.get("title", "") or ""

        # Nos quedamos solo con cosas que claramente apuntan a X/Twitter
        if "x.com" not in link and "twitter.com" not in link and "X" not in source:
            continue

        # Intentar extraer el ID del tweet desde la URL: .../status/<ID>
        created_at_str = ""
        tweet_date_ok = True  # por defecto no filtramos

        tweet_id = None
        # ejemplos de links:
        # https://x.com/USER/status/1994382410356600943
        # https://twitter.com/USER/status/1994382410356600943
        try:
            parts = link.split("/")
            if "status" in parts:
                idx = parts.index("status")
                if idx + 1 < len(parts):
                    tweet_id_str = parts[idx + 1].split("?")[0]
                    if tweet_id_str.isdigit():
                        tweet_id = int(tweet_id_str)
        except Exception:
            tweet_id = None

        tweet_dt = None
        if tweet_id is not None:
            tweet_dt = tweet_id_to_datetime(tweet_id)
            if tweet_dt is not None:
                created_at_str = tweet_dt.isoformat()

                # Si queremos que el endpoint respete el parámetro `date`:
                try:
                    target_date = datetime.strptime(date, "%Y-%m-%d").date()
                    tweet_date_ok = (tweet_dt.date() == target_date)
                except ValueError:
                    tweet_date_ok = True  # si la fecha viene mal, no filtramos

        # Si logramos parsear fecha y no coincide con la solicitada, lo filtramos
        if tweet_dt is not None and not tweet_date_ok:
            continue

        tweets.append(
            TweetItem(
                user=title or source or "desconocido",
                text=snippet or title,
                created_at=created_at_str,  # ahora sí tenemos fecha/hora cuando es posible
                url=link,
            )
        )


    return TweetsResponse(
        keyword=keyword,
        date=date,
        tweets=tweets,
    )



# ---------- ROOT ----------

@app.get("/")
def read_root():
    return {"message": "API de métricas, noticias y tweets para tu agente GPT."}

