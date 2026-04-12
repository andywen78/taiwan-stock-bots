#!/usr/bin/env python3
"""Fetch institutional trading data for bullish filter top stocks and push LINE ranking.

Runs after run_filter.py. Reads history/latest.json for stock list,
fetches 5-day institutional data from TWSE/TPEX, ranks by net buying,
and sends a LINE push notification.
"""
import os, sys, json, ssl, time, urllib.request, urllib.error
from datetime import date, timedelta

sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LATEST_TOP = os.path.join(SCRIPT_DIR, 'history', 'latest.json')
LOOKBACK_CALENDAR_DAYS = 10

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def load_line_config():
    return os.environ.get('LINE_CHANNEL_TOKEN'), os.environ.get('LINE_USER_ID')


def load_top_stocks():
    with open(LATEST_TOP, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['top'], data['date']


def _parse_int(s):
    return int(str(s).replace(',', ''))


def fetch_twse_inst(date_str):
    """Fetch TWSE institutional trading for all stocks on date_str (YYYYMMDD)."""
    url = 'https://www.twse.com.tw/fund/T86?response=json&date=' + date_str + '&selectType=ALL'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read())
        if data.get('stat') != 'OK':
            return None
        result = {}
        for row in data.get('data', []):
            code = row[0].strip()
            if len(code) != 4:
                continue
            try:
                foreign = _parse_int(row[4])   # 外陸資買賣超(不含外資自營商)
                trust = _parse_int(row[10])     # 投信買賣超
                total = _parse_int(row[17])     # 三大法人買賣超
                dealer = total - foreign - trust
                result[code] = {'foreign': foreign, 'trust': trust,
                                'dealer': dealer, 'total': total}
            except Exception:
                pass
        return result
    except Exception as e:
        print(f'  TWSE {date_str} error: {e}')
        return None


def fetch_tpex_inst(date_str):
    """Fetch TPEX institutional trading for all stocks on date_str (YYYYMMDD)."""
    y = int(date_str[:4]) - 1911
    roc = str(y) + '/' + date_str[4:6] + '/' + date_str[6:8]
    url = ('https://www.tpex.org.tw/web/stock/3insti/daily_trade/'
           '3itrade_hedge_result.php?l=zh-tw&d=' + roc + '&se=EW&t=D')
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read())
        rows = data.get('aaData', [])
        if not rows:
            return None
        result = {}
        for row in rows:
            code = str(row[0]).strip()
            if len(code) != 4:
                continue
            try:
                foreign = _parse_int(row[4])
                trust = _parse_int(row[10])
                total = _parse_int(row[17])
                dealer = total - foreign - trust
                result[code] = {'foreign': foreign, 'trust': trust,
                                'dealer': dealer, 'total': total}
            except Exception:
                pass
        return result
    except Exception as e:
        print(f'  TPEX {date_str} error: {e}')
        return None


def fetch_institutional_data(target_codes, num_days=5):
    """Fetch institutional data for the last num_days trading days."""
    all_days = {}
    d = date.today()
    attempts = 0
    while len(all_days) < num_days and attempts < LOOKBACK_CALENDAR_DAYS:
        if d.weekday() < 5:
            ds = d.strftime('%Y%m%d')
            print(f'  Fetching {ds}...')
            twse = fetch_twse_inst(ds)
            time.sleep(2)
            tpex = fetch_tpex_inst(ds)
            time.sleep(2)

            if twse is not None or tpex is not None:
                merged = {}
                if twse:
                    merged.update(twse)
                if tpex:
                    merged.update(tpex)
                found = sum(1 for c in target_codes if c in merged)
                if found > 0:
                    all_days[ds] = merged
                    print(f'    -> {ds}: {found}/{len(target_codes)} stocks found')
        d -= timedelta(days=1)
        attempts += 1
    return all_days


def aggregate(target_codes, all_days):
    """Aggregate institutional data across days for target codes."""
    agg = {}
    for code in target_codes:
        agg[code] = {'foreign': 0, 'trust': 0, 'dealer': 0, 'total': 0, 'days': 0}
        for ds in sorted(all_days):
            if code in all_days[ds]:
                for k in ['foreign', 'trust', 'dealer', 'total']:
                    agg[code][k] += all_days[ds][code][k]
                agg[code]['days'] += 1
    return agg


def stars(total_shares):
    lots = total_shares / 1000
    if lots >= 10000:
        return '\u2605\u2605\u2605\u2605\u2605'
    if lots >= 2000:
        return '\u2605\u2605\u2605\u2605'
    if lots >= 500:
        return '\u2605\u2605\u2605'
    if lots > 0:
        return '\u2605\u2605'
    if lots == 0:
        return '\u2605'
    return '\u2606'


def fmt_lots(shares):
    lots = shares / 1000
    if abs(lots) >= 10000:
        return f'{lots / 10000:+,.1f}萬張'
    return f'{lots:+,.0f}張'


def format_ranking(top_stocks, agg, filter_date, num_days):
    lines = ['\U0001f4ca 強中之強 籌碼排名 ' + filter_date, '']
    lines.append(f'依近 {num_days} 日三大法人買賣超排序')
    lines.append('')

    ranked = sorted(top_stocks,
                    key=lambda s: agg.get(s['code'], {}).get('total', 0),
                    reverse=True)

    bullish = []
    bearish = []
    for s in ranked:
        d = agg.get(s['code'], {'foreign': 0, 'trust': 0, 'total': 0, 'days': 0})
        if d['total'] >= 0:
            bullish.append((s, d))
        else:
            bearish.append((s, d))

    idx = 1
    if bullish:
        lines.append('\U0001f7e2 法人力挺')
        for s, d in bullish:
            star = stars(d['total'])
            lines.append(f"{idx}. {s['code']} {s['name']} {star}")
            lines.append(f"   外資{fmt_lots(d['foreign'])} 投信{fmt_lots(d['trust'])} "
                         f"合計{fmt_lots(d['total'])}")
            idx += 1
        lines.append('')

    if bearish:
        lines.append('\U0001f534 法人偏賣')
        for s, d in bearish:
            star = stars(d['total'])
            lines.append(f"{idx}. {s['code']} {s['name']} {star}")
            lines.append(f"   外資{fmt_lots(d['foreign'])} 投信{fmt_lots(d['trust'])} "
                         f"合計{fmt_lots(d['total'])}")
            idx += 1

    lines.append('')
    lines.append('\U0001f4a1 法人認同度越高 中線勝率越高')
    return '\n'.join(lines)


def send_line(text, token, user_id):
    url = 'https://api.line.me/v2/bot/message/push'
    payload = json.dumps({
        'to': user_id,
        'messages': [{'type': 'text', 'text': text}]
    }).encode('utf-8')
    req = urllib.request.Request(url, data=payload, method='POST')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Authorization', 'Bearer ' + token)
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            return r.status, r.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')
    except Exception as e:
        return -1, str(e)


def main():
    print('=== Institutional Ranking Analysis ===')

    if not os.path.exists(LATEST_TOP):
        print('No latest.json found, skip')
        return 0

    top_stocks, filter_date = load_top_stocks()
    if not top_stocks:
        print('No stocks in latest.json, skip')
        return 0

    codes = [s['code'] for s in top_stocks]
    print(f'Analyzing {len(codes)} stocks: {", ".join(codes)}')

    print('Fetching institutional data...')
    all_days = fetch_institutional_data(codes)
    num_days = len(all_days)
    print(f'Got {num_days} trading days of data')

    if num_days == 0:
        print('No institutional data available, skip')
        return 0

    agg = aggregate(codes, all_days)

    for code in codes:
        d = agg[code]
        f_lots = d['foreign'] / 1000
        t_lots = d['trust'] / 1000
        total_lots = d['total'] / 1000
        name = next((s['name'] for s in top_stocks if s['code'] == code), code)
        print(f"  {code} {name}: foreign={f_lots:+,.0f} trust={t_lots:+,.0f} total={total_lots:+,.0f}")

    msg = format_ranking(top_stocks, agg, filter_date, num_days)
    print('\n--- Ranking message ---')
    print(msg)

    token, uid = load_line_config()
    if not token or not uid:
        print('ERROR: LINE credentials not set')
        return 1

    status, body = send_line(msg, token, uid)
    print(f'\nLINE push status={status} body={body[:150]}')
    return 0 if status == 200 else 1


if __name__ == '__main__':
    sys.exit(main())
