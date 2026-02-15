import csv
import logging
import statistics
import threading
import time
from contextlib import asynccontextmanager
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "ticker.db"
CSV_PATH = BASE_DIR / "companies.csv"
REFERENCE_DATE = date(2025, 11, 3)  # First trading day in Nov 2025 (Monday)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------


def load_tickers(path: Path) -> dict[str, str]:
    """Return {ticker: company_name} from CSV."""
    if not path.exists():
        raise FileNotFoundError(f"Cannot find {path}")
    with open(path) as f:
        reader = csv.DictReader(f)
        return {
            row["Ticker"].strip(): row["Company"].strip()
            for row in reader
            if row["Ticker"].strip()
        }


TICKERS: dict[str, str] = load_tickers(CSV_PATH)
log.info("Loaded %d tickers from %s", len(TICKERS), CSV_PATH)

# Top tech / FAANG (separate section on dashboard)
TOP_TICKERS: dict[str, str] = {
    "META": "Meta Platforms",
    "AAPL": "Apple",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "NVDA": "NVIDIA",
    "NFLX": "Netflix",
    "AMD": "AMD",
    "INTC": "Intel",
    "TSM": "TSMC",
    "TSLA": "Tesla",
    "AVGO": "Broadcom",
    "SSNLF": "Samsung",
    "CSCO": "Cisco",
}

# Index benchmarks (not in any table — header badges + chart overlays)
INDEX_TICKERS: dict[str, str] = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq",
    "DIA": "Dow",
}

# All tickers we fetch data for (SaaS + Top + indices)
ALL_TICKERS: dict[str, str] = {**TICKERS, **TOP_TICKERS, **INDEX_TICKERS}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Price(Base):
    __tablename__ = "prices"
    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_ticker_date"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False, index=True)
    company = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    market_cap = Column(Float, nullable=True)
    all_time_high = Column(Float, nullable=True)
    fetched_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------


def _fetch_reference_prices(session: Session) -> None:
    """Fetch Nov 3, 2025 (first trading day in Nov) close for all tickers."""
    existing = {
        row.ticker
        for row in session.query(Price.ticker)
        .filter(Price.date == REFERENCE_DATE)
        .all()
    }
    missing = [t for t in ALL_TICKERS if t not in existing]
    if not missing:
        return

    log.info("Fetching reference prices (Nov 3 2025) for %d tickers", len(missing))
    for ticker in missing:
        try:
            hist = yf.Ticker(ticker).history(start="2025-11-03", end="2025-11-04")
            if hist.empty:
                log.warning("%s: no data for reference date", ticker)
                continue
            row = hist.iloc[0]
            price = Price(
                ticker=ticker,
                company=ALL_TICKERS[ticker],
                date=REFERENCE_DATE,
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=int(row["Volume"]),
                fetched_at=datetime.now(timezone.utc),
            )
            session.add(price)
            time.sleep(0.3)
        except Exception as e:
            log.error("%s: reference fetch failed: %s", ticker, e)
    session.commit()


def _backfill_history(session: Session) -> None:
    """Bulk-fetch daily prices from Nov 3 2025 → today if DB is sparse."""
    # Check for tickers that have fewer than 5 rows (need backfill)
    ticker_counts = dict(
        session.query(Price.ticker, func.count(Price.id)).group_by(Price.ticker).all()
    )
    tickers_needing_backfill = [t for t in ALL_TICKERS if ticker_counts.get(t, 0) < 5]

    if not tickers_needing_backfill:
        log.info(
            "Backfill skipped: all %d tickers have sufficient data", len(ALL_TICKERS)
        )
        return

    log.info(
        "Backfill: %d tickers need history (e.g. %s)",
        len(tickers_needing_backfill),
        ", ".join(tickers_needing_backfill[:5]),
    )
    ticker_list = tickers_needing_backfill
    end_date = (date.today() + timedelta(days=1)).isoformat()

    try:
        df = yf.download(
            ticker_list,
            start="2025-11-03",
            end=end_date,
            group_by="ticker",
            auto_adjust=True,
            threads=True,
        )
    except Exception as e:
        log.error("Backfill bulk download failed: %s", e)
        return

    if df.empty:
        log.warning("Backfill: download returned empty DataFrame")
        return

    # Pre-load existing (ticker, date) pairs for O(1) skip
    existing = set(session.query(Price.ticker, Price.date).all())

    rows_to_insert = []
    multi_ticker = len(ticker_list) > 1

    for ticker in ticker_list:
        company = ALL_TICKERS[ticker]
        try:
            ticker_df = df[ticker] if multi_ticker else df
        except KeyError:
            log.warning("Backfill: %s not in download results", ticker)
            continue

        for dt_idx, row in ticker_df.iterrows():
            trade_date = dt_idx.date()
            if (ticker, trade_date) in existing:
                continue
            close_val = row.get("Close")
            if close_val is None or pd.isna(close_val):
                continue

            rows_to_insert.append(
                Price(
                    ticker=ticker,
                    company=company,
                    date=trade_date,
                    open=round(float(row["Open"]), 2)
                    if not pd.isna(row.get("Open"))
                    else None,
                    high=round(float(row["High"]), 2)
                    if not pd.isna(row.get("High"))
                    else None,
                    low=round(float(row["Low"]), 2)
                    if not pd.isna(row.get("Low"))
                    else None,
                    close=round(float(close_val), 2),
                    volume=int(row["Volume"])
                    if not pd.isna(row.get("Volume"))
                    else None,
                    market_cap=None,
                    all_time_high=None,
                    fetched_at=datetime.now(timezone.utc),
                )
            )

    if rows_to_insert:
        session.bulk_save_objects(rows_to_insert)
        session.commit()
        log.info("Backfill complete: inserted %d rows", len(rows_to_insert))
    else:
        log.info("Backfill: no new rows to insert")


def fetch_all_prices() -> dict:
    """Fetch latest prices + metadata for all tickers. Returns summary."""
    results: dict = {"fetched": 0, "skipped": [], "errors": []}
    session = SessionLocal()
    try:
        # Backfill historical data if sparse (runs once)
        _backfill_history(session)

        # Ensure reference prices exist
        _fetch_reference_prices(session)

        for ticker in ALL_TICKERS:
            try:
                yf_ticker = yf.Ticker(ticker)
                is_index = ticker in INDEX_TICKERS

                # Latest day price
                hist = yf_ticker.history(period="5d")
                if hist.empty:
                    results["skipped"].append(ticker)
                    continue

                row = hist.iloc[-1]
                trade_date = hist.index[-1].date()

                # Skip if already stored
                exists = (
                    session.query(Price)
                    .filter_by(ticker=ticker, date=trade_date)
                    .first()
                )
                if exists:
                    # Update market_cap and all_time_high if missing (skip for indices)
                    if not is_index and (
                        exists.market_cap is None or exists.all_time_high is None
                    ):
                        try:
                            info = yf_ticker.info
                            if exists.market_cap is None:
                                exists.market_cap = info.get("marketCap")
                            if exists.all_time_high is None:
                                exists.all_time_high = info.get("fiftyTwoWeekHigh")
                        except Exception:
                            pass
                    results["skipped"].append(ticker)
                    continue

                # Get market cap and ATH from info (skip for indices)
                market_cap = None
                all_time_high = None
                if not is_index:
                    try:
                        info = yf_ticker.info
                        market_cap = info.get("marketCap")
                        all_time_high = info.get("fiftyTwoWeekHigh")
                    except Exception:
                        pass

                price = Price(
                    ticker=ticker,
                    company=ALL_TICKERS[ticker],
                    date=trade_date,
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=int(row["Volume"]),
                    market_cap=market_cap,
                    all_time_high=all_time_high,
                    fetched_at=datetime.now(timezone.utc),
                )
                session.add(price)
                results["fetched"] += 1
                time.sleep(0.3)
            except Exception as e:
                results["errors"].append(f"{ticker}: {e}")
                log.error("%s: %s", ticker, e)

        session.commit()
    finally:
        session.close()

    log.info(
        "Fetch complete: %d fetched, %d skipped, %d errors",
        results["fetched"],
        len(results["skipped"]),
        len(results["errors"]),
    )
    return results


# ---------------------------------------------------------------------------
# Heatmap color helpers
# ---------------------------------------------------------------------------


def _heatmap_nov1(pct: float) -> dict:
    """Return {'bg': ..., 'fg': ...} for % since Nov 1."""
    if pct < 0:
        intensity = min(abs(pct) / 70, 1.0) * 0.6
        bg = f"rgba(220,50,47,{intensity:.2f})"
    else:
        intensity = min(pct / 30, 1.0) * 0.55
        bg = f"rgba(40,167,69,{intensity:.2f})"
    fg = "#fff" if intensity > 0.25 else "#c9d1d9"
    return {"bg": bg, "fg": fg}


def _heatmap_ath(pct: float) -> dict:
    """Return {'bg': ..., 'fg': ...} for % since ATH."""
    intensity = min(abs(pct) / 85, 1.0) * 0.55
    bg = f"rgba(220,50,47,{intensity:.2f})"
    fg = "#fff" if intensity > 0.25 else "#c9d1d9"
    return {"bg": bg, "fg": fg}


def _fmt_mcap(val: float | None) -> str:
    """Format market cap to abbreviated string."""
    if val is None:
        return "N/A"
    if val >= 1e12:
        return f"${val / 1e12:.1f}T"
    if val >= 1e9:
        return f"${val / 1e9:.1f}B"
    if val >= 1e6:
        return f"${val / 1e6:.0f}M"
    return f"${val:,.0f}"


# ---------------------------------------------------------------------------
# Dashboard data builder
# ---------------------------------------------------------------------------


def build_dashboard_data() -> dict:
    """Query DB and build template context."""
    session = SessionLocal()
    try:
        # Get the most recent date per ticker
        latest_sub = (
            session.query(Price.ticker, func.max(Price.date).label("max_date"))
            .filter(Price.date != REFERENCE_DATE)
            .group_by(Price.ticker)
            .subquery()
        )

        latest_prices = (
            session.query(Price)
            .join(
                latest_sub,
                (Price.ticker == latest_sub.c.ticker)
                & (Price.date == latest_sub.c.max_date),
            )
            .all()
        )

        # Get reference prices (Nov 1 2025)
        ref_prices = {
            p.ticker: p.close
            for p in session.query(Price).filter(Price.date == REFERENCE_DATE).all()
        }

        top_rows = []
        saas_rows = []
        index_pcts: dict[str, float | None] = {}  # ticker -> % since Nov 3

        def _make_row(p, ref_close, pct_nov1):
            pct_ath = (
                ((p.close - p.all_time_high) / p.all_time_high * 100)
                if p.all_time_high
                else None
            )
            return {
                "ticker": p.ticker,
                "company": p.company,
                "close": p.close,
                "close_fmt": f"${p.close:,.2f}",
                "market_cap": p.market_cap,
                "mcap_fmt": _fmt_mcap(p.market_cap),
                "pct_nov1": pct_nov1,
                "pct_nov1_fmt": f"{pct_nov1:+.1f}%" if pct_nov1 is not None else "N/A",
                "pct_nov1_style": _heatmap_nov1(pct_nov1)
                if pct_nov1 is not None
                else {"bg": "transparent", "fg": "#c9d1d9"},
                "ref_close_fmt": f"${ref_close:,.2f}" if ref_close else "N/A",
                "pct_ath": pct_ath,
                "pct_ath_fmt": f"{pct_ath:+.1f}%" if pct_ath is not None else "N/A",
                "pct_ath_style": _heatmap_ath(pct_ath)
                if pct_ath is not None
                else {"bg": "transparent", "fg": "#c9d1d9"},
                "ath_price_fmt": f"${p.all_time_high:,.2f}"
                if p.all_time_high
                else "N/A",
                "date": p.date,
            }

        for p in latest_prices:
            ref_close = ref_prices.get(p.ticker)
            pct_nov1 = ((p.close - ref_close) / ref_close * 100) if ref_close else None

            if p.ticker in INDEX_TICKERS:
                index_pcts[p.ticker] = pct_nov1
            elif p.ticker in TOP_TICKERS:
                top_rows.append(_make_row(p, ref_close, pct_nov1))
            else:
                saas_rows.append(_make_row(p, ref_close, pct_nov1))

        # Sort both sections by % since Nov 3 ascending (worst first)
        _sort_key = lambda r: r["pct_nov1"] if r["pct_nov1"] is not None else 0
        top_rows.sort(key=_sort_key)
        saas_rows.sort(key=_sort_key)

        # Compute medians per section
        def _medians(rows):
            n1 = [r["pct_nov1"] for r in rows if r["pct_nov1"] is not None]
            at = [r["pct_ath"] for r in rows if r["pct_ath"] is not None]
            return (
                statistics.median(n1) if n1 else None,
                statistics.median(at) if at else None,
            )

        top_med_nov1, top_med_ath = _medians(top_rows)
        saas_med_nov1, saas_med_ath = _medians(saas_rows)

        # Build index benchmark data for header
        def _fmt_pct(pct: float | None) -> str:
            return f"{pct:+.1f}%" if pct is not None else "N/A"

        indices = [
            {
                "ticker": t,
                "label": INDEX_TICKERS[t],
                "pct": index_pcts.get(t),
                "pct_fmt": _fmt_pct(index_pcts.get(t)),
            }
            for t in INDEX_TICKERS
        ]

        # Get last fetched_at
        last_updated = session.query(func.max(Price.fetched_at)).scalar()

        return {
            "top_rows": top_rows,
            "rows": saas_rows,
            "indices": indices,
            "top_med_nov1": top_med_nov1,
            "top_med_nov1_fmt": _fmt_pct(top_med_nov1),
            "top_med_ath": top_med_ath,
            "top_med_ath_fmt": _fmt_pct(top_med_ath),
            "saas_med_nov1": saas_med_nov1,
            "saas_med_nov1_fmt": _fmt_pct(saas_med_nov1),
            "saas_med_ath": saas_med_ath,
            "saas_med_ath_fmt": _fmt_pct(saas_med_ath),
            "last_updated": last_updated.strftime("%b %d, %Y %H:%M UTC")
            if last_updated
            else "Never",
            "top_count": len(TOP_TICKERS),
            "saas_count": len(TICKERS),
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_all_prices,
    trigger="cron",
    hour=18,
    minute=0,
    day_of_week="mon-fri",
    misfire_grace_time=3600,
    id="daily_price_fetch",
    replace_existing=True,
)


# ---------------------------------------------------------------------------
# Fetch state (shared across requests)
# ---------------------------------------------------------------------------
_fetch_lock = threading.Lock()
_fetching = False


def _run_fetch_in_background():
    global _fetching
    try:
        fetch_all_prices()
    finally:
        with _fetch_lock:
            _fetching = False


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    scheduler.start()
    log.info("Scheduler started (daily 6 PM Mon-Fri)")
    yield
    scheduler.shutdown(wait=False)
    log.info("Scheduler shut down")


app = FastAPI(title="Death of SaaS?", lifespan=lifespan)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    data = build_dashboard_data()
    data["is_fetching"] = _fetching
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, **data},
    )


@app.post("/fetch")
async def trigger_fetch():
    global _fetching
    with _fetch_lock:
        if not _fetching:
            _fetching = True
            thread = threading.Thread(target=_run_fetch_in_background, daemon=True)
            thread.start()
    return RedirectResponse(url="/", status_code=303)


@app.get("/status")
async def fetch_status():
    return {"is_fetching": _fetching}


@app.get("/api/chart/median-history")
async def chart_median_history(metric: str = "nov3"):
    """Return daily median % and per-ticker % for chart rendering."""
    session = SessionLocal()
    try:
        # 1. Get all daily prices from reference date onward
        rows = (
            session.query(Price.ticker, Price.company, Price.date, Price.close)
            .filter(Price.date >= REFERENCE_DATE)
            .order_by(Price.date)
            .all()
        )

        # 2. Build reference map
        if metric == "ath":
            # Use the most recent all_time_high per ticker
            ath_rows = (
                session.query(Price.ticker, Price.all_time_high)
                .filter(Price.all_time_high.isnot(None))
                .all()
            )
            ref_map: dict[str, float] = {}
            for r in ath_rows:
                # Keep the max ATH seen for each ticker
                if r.ticker not in ref_map or r.all_time_high > ref_map[r.ticker]:
                    ref_map[r.ticker] = float(r.all_time_high)
        else:
            # Use Nov 3 close as reference
            ref_rows = (
                session.query(Price.ticker, Price.close)
                .filter(Price.date == REFERENCE_DATE)
                .all()
            )
            ref_map = {r.ticker: float(r.close) for r in ref_rows if r.close}

        # 3. Group by date, compute % change per ticker (separate SaaS vs indices)
        date_ticker_pct: dict[date, dict[str, float]] = defaultdict(dict)
        date_index_pct: dict[date, dict[str, float]] = defaultdict(dict)
        company_map: dict[str, str] = {}

        for ticker, company, dt, close in rows:
            if close is None or ticker not in ref_map:
                continue
            ref_val = ref_map[ticker]
            if not ref_val:
                continue
            pct = (float(close) - ref_val) / ref_val * 100
            if ticker in INDEX_TICKERS:
                date_index_pct[dt][ticker] = round(pct, 2)
            else:
                date_ticker_pct[dt][ticker] = round(pct, 2)
                company_map[ticker] = company

        # 4. Build response arrays
        sorted_dates = sorted(set(date_ticker_pct.keys()) | set(date_index_pct.keys()))
        all_tickers = sorted(company_map.keys())

        median_values: list[float | None] = []
        ticker_values: dict[str, list[float | None]] = {t: [] for t in all_tickers}
        index_values: dict[str, list[float | None]] = {t: [] for t in INDEX_TICKERS}

        for dt in sorted_dates:
            day_pcts = date_ticker_pct.get(dt, {})
            values = list(day_pcts.values())
            median_values.append(
                round(statistics.median(values), 2) if values else None
            )
            for t in all_tickers:
                ticker_values[t].append(day_pcts.get(t))
            day_idx = date_index_pct.get(dt, {})
            for t in INDEX_TICKERS:
                index_values[t].append(day_idx.get(t))

        return {
            "metric": metric,
            "dates": [d.isoformat() for d in sorted_dates],
            "median": median_values,
            "tickers": {
                t: {"company": company_map[t], "values": ticker_values[t]}
                for t in all_tickers
            },
            "indices": {
                t: {"label": INDEX_TICKERS[t], "values": index_values[t]}
                for t in INDEX_TICKERS
            },
        }
    finally:
        session.close()
