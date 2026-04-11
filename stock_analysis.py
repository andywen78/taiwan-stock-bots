#!/usr/bin/env python3
"""Taiwan stock daily holdings analysis.

Reads holdings from Google Sheet, fetches TWSE/TPEX data,
composes report, pushes to LINE.

Env vars: LINE_CHANNEL_TOKEN, LINE_USER_ID
"""
import os, sys, csv, io, json, ssl, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

sys.stdout.reconfigure(encoding='utf-8')

SHEET_ID = '1nOcCzOzWrtDk0bljSU9wcUkCXUI-V-KAt6nwJ5oa74c'
SHEET_GID = '0'

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read()

def today_tw():
    return datetime.now(timezone(timedelta(hours=8)))

def fetch_holdings():
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}'
    body = http_get(url).decode('utf-8-sig')
    holdings = []
    for i, row in enumerate(csv.reader(io.StringIO(body))):
        if i == 0 or not row or not row[0].strip():
            continue
        code = row[0].strip()
        if not (code.isdigit() and len(code) == 4):
            continue
        holdings.append({
            'code': code,
            'name': row[1].strip() if len(row) > 1 else '',
            'note': row[2].strip() if len(row) > 2 else '',
        })
    return holdings

def to_lots(shares_str):
    try:
        return int(str(shares_str).replace(',', '')) // 1000
    except Exception:
        return None

def fetch_twse_stock(code, yyyymmdd):
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={yyyymmdd}&stockNo={code}&response=json'
    try:
        data = json.loads(http_get(url))
        rows = data.get('data') or []
        if not rows:
            return None
        last = rows[-1]
        return {
            'market': 'TWSE',
            'date': last[0],
            'vol_lots': to_lots(last[1]),
            'open': float(last[3].replace(',', '')),
            'high': float(last[4].replace(',', '')),
            'low': float(last[5].replace(',', '')),
            'close': float(last[6].replace(',', '')),
            'change': last[7].strip(),
        }
    except Exception as e:
        print(f'  twse {code} ERR: {e}')
        return None

def fetch_tpex_all(roc_date):
    """Fetch all OTC stocks in one call, return dict {code: {...}}."""
    url = (f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/'
           f'stk_wn1430_result.php?l=zh-tw&d={roc_date}&se=AL&s=0,asc,0')
    result = {}
    try:
        data = json.loads(http_get(url))
        tables = data.get('tables', [])
        rows = tables[0].get('data', []) if tables else []
        for row in rows:
            if len(row) < 8:
                continue
            code = row[0].strip()
            try:
                result[code] = {
                    'market': 'TPEX',
                    'close': float(row[2].replace(',', '')),
                    'change': row[3].strip(),
                    'open': float(row[4].replace(',', '')),
                    'high': float(row[5].replace(',', '')),
                    'low': float(row[6].replace(',', '')),
                    'vol_lots': to_lots(row[7]),
                }
            except Exception:
                pass
    except Exception as e:
        print(f'  tpex all ERR: {e}')
    return result

def fetch_twse_fund(yyyymmdd):
    """TWSE T86 institutional net buy/sell. Return dict {code: {foreign, trust}} in lots."""
    url = f'https://www.twse.com.tw/rwd/zh/fund/T86?date={yyyymmdd}&selectType=ALLBUT0999&response=json'
    result = {}
    try:
        data = json.loads(http_get(url))
        for row in data.get('data') or []:
            code = row[0].strip()
            try:
                result[code] = {
                    'foreign': to_lots(row[4]),
                    'trust': to_lots(row[10]),
                }
            except Exception:
                pass
    except Exception as e:
        print(f'  twse fund ERR: {e}')
    return result

def fetch_tpex_fund(roc_date):
    """TPEX 3insti. Return dict {code: {foreign, trust}} in lots."""
    url = (f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/'
           f'3itrade_hedge_result.php?l=zh-tw&t=D&d={roc_date}&s=0,asc')
    result = {}
    try:
        data = json.loads(http_get(url))
        tables = data.get('tables', [])
        rows = tables[0].get('data', []) if tables else []
        for row in rows:
            if len(row) < 14:
                continue
            code = row[0].strip()
            try:
                result[code] = {
                    'foreign': to_lots(row[4]),
                    'trust': to_lots(row[13]),
                }
            except Exception:
                pass
    except Exception as e:
        print(f'  tpex fund ERR: {e}')
    return result

def fetch_market(yyyymmdd):
    """TWSE market summary. Return {rise, fall, vol_yi} or None if holiday."""
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={yyyymmdd}&type=MS&response=json'
    try:
        data = json.loads(http_get(url))
        if data.get('stat') != 'OK':
            return None
        rise = fall = vol_yi = None
        for t in data.get('tables', []):
            for row in t.get('data', []):
                if not row:
                    continue
                label = str(row[0])
                if '一般股票' in label and vol_yi is None and len(row) >= 2:
                    try:
                        vol_yi = int(row[1].replace(',', '')) // 100_000_000
                    except Exception:
                        pass
                elif '上漲' in label and rise is None and len(row) >= 3:
                    try:
                        rise = int(row[2].split('(')[0].replace(',', ''))
                    except Exception:
                        pass
                elif '下跌' in label and fall is None and len(row) >= 3:
                    try:
                        fall = int(row[2].split('(')[0].replace(',', ''))
                    except Exception:
                        pass
        if rise is None or fall is None:
            return None
        return {'rise': rise, 'fall': fall, 'vol_yi': vol_yi}
    except Exception as e:
        print(f'  market ERR: {e}')
        return None

def suggest(change_str, foreign, trust):
    try:
        ch = float(str(change_str).replace('+', '').replace(',', ''))
    except Exception:
        ch = 0.0
    f = foreign if foreign is not None else 0
    t = trust if trust is not None else 0
    if ch > 2:
        return '價漲，觀察量能是否配合'
    if ch < -3:
        return '跌幅較大，注意支撐'
    if f > 500 and t >= 0:
        return '外資大買，可續抱'
    if f < -500:
        return '外資大賣，注意減碼'
    return '區間震盪，持續觀察'

def sign(n):
    if n is None:
        return None
    return f'{n:+d}'

def compose_message(holdings, stocks, market, date_display):
    lines = [f'📊 台股持股分析 {date_display}']
    if market:
        vol = f"成交{market['vol_yi']}億" if market.get('vol_yi') else ''
        lines.append(f"[大盤] 漲{market['rise']}/跌{market['fall']} {vol}".rstrip())
    else:
        lines.append('[大盤] 資料不可取得')
    for h in holdings:
        s = stocks.get(h['code'])
        lines.append('')
        lines.append(f"{h['code']} {h['name']}")
        if not s:
            lines.append('（股價抓取失敗）')
            if h.get('note'):
                lines.append(f"備註：{h['note']}")
            continue
        lines.append(f"收盤 {s['close']} ({s['change']}) 量 {s['vol_lots']}張")
        parts = []
        f = s.get('foreign'); t = s.get('trust')
        if f is not None:
            parts.append(f"外資{sign(f)}張")
        if t is not None:
            parts.append(f"投信{sign(t)}張")
        if parts:
            lines.append(' / '.join(parts))
        lines.append(suggest(s['change'], f, t))
        if h.get('note'):
            lines.append(f"備註：{h['note']}")
    return '\n'.join(lines)

def send_line(text):
    token = os.environ.get('LINE_CHANNEL_TOKEN')
    user = os.environ.get('LINE_USER_ID')
    if not token or not user:
        print('ERROR: LINE_CHANNEL_TOKEN / LINE_USER_ID not set')
        return False
    payload = json.dumps({
        'to': user,
        'messages': [{'type': 'text', 'text': text}],
    }).encode('utf-8')
    req = urllib.request.Request(
        'https://api.line.me/v2/bot/message/push',
        data=payload, method='POST',
    )
    req.add_header('Content-Type', 'application/json')
    req.add_header('Authorization', f'Bearer {token}')
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            body = r.read().decode('utf-8', errors='replace')
            print(f'LINE push status={r.status} body={body[:200]}')
            return r.status == 200
    except urllib.error.HTTPError as e:
        print(f'LINE HTTPError {e.code}: {e.read().decode("utf-8","replace")[:200]}')
        return False
    except Exception as e:
        print(f'LINE error: {e}')
        return False

def main():
    now = today_tw()
    yyyymmdd = now.strftime('%Y%m%d')
    display = now.strftime('%Y/%m/%d')
    roc = f'{now.year - 1911}/{now.month:02d}/{now.day:02d}'
    print(f'=== Taiwan Stock Daily ===')
    print(f'Date: {display} (YYYYMMDD={yyyymmdd}, ROC={roc})')

    holdings = fetch_holdings()
    print(f'Holdings: {len(holdings)}')
    for h in holdings:
        print(f"  {h['code']} {h['name']} | {h['note']}")
    if not holdings:
        send_line(f'台股分析 {display}\n持股清單為空')
        return 1

    market = fetch_market(yyyymmdd)
    print(f'Market: {market}')

    if market is None:
        send_line(f'台股分析 {display}\n今日休市或資料未更新')
        return 0

    twse_fund = fetch_twse_fund(yyyymmdd)
    tpex_stocks = fetch_tpex_all(roc)
    tpex_fund = fetch_tpex_fund(roc)
    print(f'TWSE fund: {len(twse_fund)}, TPEX stocks: {len(tpex_stocks)}, TPEX fund: {len(tpex_fund)}')

    stocks = {}
    for h in holdings:
        code = h['code']
        s = fetch_twse_stock(code, yyyymmdd)
        if s is None:
            s = tpex_stocks.get(code)
            if s is not None:
                s = dict(s)
        if s is None:
            print(f'  {code}: NOT FOUND')
            continue
        if s['market'] == 'TWSE':
            fund = twse_fund.get(code, {})
        else:
            fund = tpex_fund.get(code, {})
        s['foreign'] = fund.get('foreign')
        s['trust'] = fund.get('trust')
        stocks[code] = s
        print(f"  {code} {s['market']}: close={s['close']} chg={s['change']} vol={s['vol_lots']} f={s.get('foreign')} t={s.get('trust')}")

    msg = compose_message(holdings, stocks, market, display)
    print('\n--- MESSAGE ---')
    print(msg)
    print('---------------\n')

    ok = send_line(msg)
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main())
