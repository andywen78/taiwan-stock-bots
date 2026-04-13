#!/usr/bin/env python3
"""Taiwan 蟄伏雷達 (Type A quiet-accumulation) — cloud fallback.

Self-contained GitHub Actions version of the local taiwan-accumulation-radar skill.
Fetches TWSE + TPEX daily data, runs Type A screen with momentum, pushes LINE.

Reads LINE_CHANNEL_TOKEN and LINE_USER_ID from environment.
Optional: DISCORD_WEBHOOK_STOCK for Discord fallback.
"""
import os
import sys
import json
import ssl
import time
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta

sys.stdout.reconfigure(encoding='utf-8')

CACHE_DIR = '/tmp/bulk_cache_acc'
HISTORY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'history')
LATEST = os.path.join(HISTORY_DIR, 'accumulation_latest.json')
FETCH_DAYS = 100  # need 60+ trading days
SKIP_DATE = '20260223'

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ── Fetch (TWSE / TPEX) ──
def fetch_twse_day(d):
    cache = CACHE_DIR + '/twse_' + d + '.json'
    if os.path.exists(cache):
        return 'cached'
    url = ('https://www.twse.com.tw/exchangeReport/MI_INDEX?'
           'response=json&date=' + d + '&type=ALL')
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read())
        if data.get('stat') != 'OK':
            return 'holiday'
        for t in data.get('tables', []):
            if '每日收盤行情' in t.get('title', '') and len(t.get('data', [])) > 100:
                out = []
                for row in t['data']:
                    code = row[0]
                    if len(code) != 4 or code.startswith('00'):
                        continue
                    try:
                        vol = int(row[2].replace(',', ''))
                        op = float(row[5].replace(',', ''))
                        hi = float(row[6].replace(',', ''))
                        lo = float(row[7].replace(',', ''))
                        cl = float(row[8].replace(',', ''))
                        out.append([code, row[1], op, hi, lo, cl, vol])
                    except Exception:
                        pass
                with open(cache, 'w', encoding='utf-8') as f:
                    json.dump(out, f, ensure_ascii=False)
                return len(out)
        return 'no_data'
    except Exception as e:
        return 'ERR:' + str(e)[:60]


def fetch_tpex_day(d):
    cache = CACHE_DIR + '/tpex_' + d + '.json'
    if os.path.exists(cache):
        return 'cached'
    y = int(d[:4]) - 1911
    roc = str(y) + '/' + d[4:6] + '/' + d[6:8]
    url = ('https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/'
           'stk_wn1430_result.php?l=zh-tw&d=' + roc + '&se=AL&s=0,asc,0')
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read())
        tables = data.get('tables', [])
        if not tables or len(tables[0].get('data', [])) == 0:
            return 'empty'
        out = []
        for row in tables[0]['data']:
            code = row[0]
            if len(code) != 4 or code.startswith('00'):
                continue
            try:
                cl = float(row[2].replace(',', ''))
                op = float(row[4].replace(',', ''))
                hi = float(row[5].replace(',', ''))
                lo = float(row[6].replace(',', ''))
                vol = int(row[7].replace(',', ''))
                out.append([code, row[1], op, hi, lo, cl, vol])
            except Exception:
                pass
        with open(cache, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False)
        return len(out)
    except Exception:
        return 'ERR'


def incremental_update():
    start = date.today() - timedelta(days=FETCH_DAYS)
    today = date.today()
    d = start
    fetched = 0
    while d <= today:
        if d.weekday() < 5:
            ds = d.strftime('%Y%m%d')
            if ds != SKIP_DATE:
                tw = fetch_twse_day(ds)
                if tw != 'cached':
                    time.sleep(1.5)
                    fetched += 1
                tp = fetch_tpex_day(ds)
                if tp != 'cached':
                    time.sleep(0.5)
                    fetched += 1
        d += timedelta(days=1)
    print(f'Incremental fetch: {fetched} API calls')


def load_all_data_from_cache():
    all_data = {}
    for fn in sorted(os.listdir(CACHE_DIR)):
        if not fn.endswith('.json'):
            continue
        d = fn.replace('.json', '').split('_')[1]
        with open(CACHE_DIR + '/' + fn, encoding='utf-8') as f:
            rows = json.load(f)
        for row in rows:
            code, name, op, hi, lo, cl, vol = row
            if code not in all_data:
                all_data[code] = {'name': name, 'bars': []}
            all_data[code]['bars'].append({
                'd': d, 'o': op, 'h': hi, 'l': lo, 'c': cl, 'v': vol
            })
    for code in all_data:
        seen = {b['d']: b for b in all_data[code]['bars']}
        all_data[code]['bars'] = sorted(seen.values(), key=lambda x: x['d'])
    return all_data


# ── Type A screen ──
MIN_PRICE = 10
MIN_AVG5_VOL = 300
LOW_ZONE_MAX = 25
MA20_SLOPE_MIN = 0.97
MA20_SLOPE_MAX = 1.05
CL_MA20_MIN = 0.98
CL_MA20_MAX = 1.10
VOL_MIN = 0.55
VOL_MAX = 1.30
SURGE_MIN = -3
SURGE_MAX = 8
RANGE_20D_MAX = 1.22
TOP_N = 20
EXCLUDE_FINANCIAL = True


def is_financial(code):
    if not code.isdigit():
        return False
    c = int(code)
    if 2801 <= c <= 2899:
        return True
    if code in ('5820', '5876', '5880'):
        return True
    return False


def calc_ma(closes, period, idx):
    if idx < period - 1:
        return None
    return sum(closes[idx - period + 1:idx + 1]) / period


def screen_stock(bars):
    if len(bars) < 60:
        return False, 0, None
    idx = len(bars) - 1
    cl = bars[idx]['c']
    if cl < MIN_PRICE:
        return False, 0, None
    avg5v = sum(b['v'] for b in bars[-5:]) / 5 / 1000
    if avg5v < MIN_AVG5_VOL:
        return False, 0, None

    closes = [b['c'] for b in bars]
    highs = [b['h'] for b in bars]
    lows = [b['l'] for b in bars]
    vols = [b['v'] for b in bars]

    ma5 = calc_ma(closes, 5, idx)
    ma20 = calc_ma(closes, 20, idx)
    ma60 = calc_ma(closes, 60, idx)
    ma20_10_ago = calc_ma(closes, 20, idx - 10)
    if not all([ma5, ma20, ma60, ma20_10_ago]):
        return False, 0, None

    low_60 = min(lows[-60:])
    low_zone_pct = (cl / low_60 - 1) * 100 if low_60 else 999
    if low_zone_pct > LOW_ZONE_MAX:
        return False, 0, None

    ma20_slope = ma20 / ma20_10_ago
    if not (MA20_SLOPE_MIN <= ma20_slope <= MA20_SLOPE_MAX):
        return False, 0, None

    cl_over_ma20 = cl / ma20
    if not (CL_MA20_MIN <= cl_over_ma20 <= CL_MA20_MAX):
        return False, 0, None

    avg20v = sum(vols[-20:]) / 20
    avg3v = sum(vols[-3:]) / 3
    vol_ratio = avg3v / avg20v if avg20v > 0 else 0
    if not (VOL_MIN <= vol_ratio <= VOL_MAX):
        return False, 0, None

    cl_5 = closes[idx - 5] if idx >= 5 else cl
    surge_5d = (cl / cl_5 - 1) * 100 if cl_5 else 0
    if not (SURGE_MIN <= surge_5d <= SURGE_MAX):
        return False, 0, None

    high_20 = max(highs[-20:])
    low_20 = min(lows[-20:])
    range_20d = high_20 / low_20 if low_20 > 0 else 99
    if range_20d > RANGE_20D_MAX:
        return False, 0, None

    score = 3
    if 0.99 <= ma20_slope <= 1.02:
        score += 2
    if 0.70 <= vol_ratio <= 0.95:
        score += 2
    elif 0.55 <= vol_ratio < 0.70 or 0.95 < vol_ratio <= 1.15:
        score += 1
    if range_20d <= 1.08:
        score += 2
    elif range_20d <= 1.17:
        score += 1
    if 10 <= low_zone_pct <= 22:
        score += 3
    elif 5 <= low_zone_pct < 10 or 22 < low_zone_pct <= 25:
        score += 1
    high_60 = max(highs[-60:])
    range_60d = high_60 / low_60 if low_60 > 0 else 99
    if range_60d <= 1.25:
        score += 2
    elif range_60d <= 1.35:
        score += 1
    if cl >= ma20:
        score += 1
    if ma5 >= ma20:
        score += 1
    high_5 = max(highs[-5:])
    low_5 = min(lows[-5:])
    range_5d = high_5 / low_5 if low_5 > 0 else 99
    if range_20d > 1.0:
        contract = (range_5d - 1) / (range_20d - 1)
        if contract <= 0.6:
            score += 1

    return True, score, {
        'close': cl,
        'low_zone_pct': round(low_zone_pct, 1),
        'ma20_slope': round(ma20_slope, 3),
        'vol_ratio_3d': round(vol_ratio, 2),
        'surge_5d_pct': round(surge_5d, 1),
        'range_20d': round(range_20d, 2),
    }


def compute_momentum(today_rank, rank_1d_ago, rank_5d_ago):
    r1 = rank_1d_ago if rank_1d_ago is not None else 500
    r5 = rank_5d_ago if rank_5d_ago is not None else 500
    climb_1d = r1 - today_rank
    climb_5d = r5 - today_rank
    score = 0
    if climb_1d >= 50:
        score += 3
    elif climb_1d >= 20:
        score += 2
    elif climb_1d >= 5:
        score += 1
    elif climb_1d < -20:
        score -= 1
    if climb_5d >= 80:
        score += 3
    elif climb_5d >= 40:
        score += 2
    elif climb_5d >= 10:
        score += 1
    return score, climb_1d, climb_5d


def truncate_to(all_data, target_date):
    out = {}
    for code, info in all_data.items():
        bars = [b for b in info['bars'] if b['d'] <= target_date]
        if bars and bars[-1]['d'] == target_date:
            out[code] = {'name': info['name'], 'bars': bars}
    return out


def screen_market(data):
    results = []
    for code, info in data.items():
        if EXCLUDE_FINANCIAL and is_financial(code):
            continue
        ok, score, m = screen_stock(info['bars'])
        if ok:
            results.append({
                'code': code,
                'name': info['name'],
                'score': score,
                **m,
            })
    results.sort(key=lambda x: (-x['score'], x['low_zone_pct'], x['range_20d']))
    return results


def run_full_screen(full_data):
    trading = sorted({b['d'] for info in full_data.values() for b in info['bars']})
    if not trading:
        return [], None
    today = trading[-1]
    yesterday = trading[-2] if len(trading) >= 2 else None
    five_ago = trading[-6] if len(trading) >= 6 else None

    today_results = screen_market(truncate_to(full_data, today))

    y_map = {}
    if yesterday:
        y_res = screen_market(truncate_to(full_data, yesterday))
        y_map = {r['code']: i + 1 for i, r in enumerate(y_res)}
    f_map = {}
    if five_ago:
        f_res = screen_market(truncate_to(full_data, five_ago))
        f_map = {r['code']: i + 1 for i, r in enumerate(f_res)}

    for i, r in enumerate(today_results, 1):
        r['rank_1d_ago'] = y_map.get(r['code'])
        r['rank_5d_ago'] = f_map.get(r['code'])
        m, c1, c5 = compute_momentum(i, r['rank_1d_ago'], r['rank_5d_ago'])
        r['momentum'] = m

    today_results.sort(
        key=lambda x: (-x['score'], -x['momentum'], x['low_zone_pct'])
    )
    for i, r in enumerate(today_results, 1):
        r['today_rank'] = i
    return today_results, today


# ── Message format ──
WEEKDAY_CH = ['一', '二', '三', '四', '五', '六', '日']


def _arrow_tag(r):
    r1 = r['rank_1d_ago']
    if r1 is None:
        return '首次'
    d = r1 - r['today_rank']
    if d > 0:
        return f'↑{d}位'
    if d < 0:
        return f'↓{-d}位'
    return '持平'


def _is_sprinter(r):
    return r['momentum'] >= 4 or r['rank_1d_ago'] is None


def _date_header(today):
    date_str = f'{today[:4]}-{today[4:6]}-{today[6:8]}'
    wd = WEEKDAY_CH[datetime.strptime(today, '%Y%m%d').weekday()]
    return date_str, wd


def _line_stock_block(r):
    sprint = _is_sprinter(r)
    new_in = r['rank_1d_ago'] is None
    badge = ''
    if new_in:
        badge = '  🚀新進榜'
    elif sprint:
        badge = '  🚀衝刺'
    rank = r['today_rank']
    return (
        f'  #{rank:<2} {r["code"]} {r["name"]}{badge}\n'
        f'     🎯蟄伏{r["score"]}分  📈動能{r["momentum"]:+d}  💰{r["close"]}元\n'
        f'     📉離底+{r["low_zone_pct"]}%  📊量{r["vol_ratio_3d"]}  ⬆️昨{_arrow_tag(r)}'
    )


def _dc_stock_block(r):
    sprint = _is_sprinter(r)
    new_in = r['rank_1d_ago'] is None
    badge = ''
    if new_in:
        badge = ' · ✨ **新進榜**'
    elif sprint:
        badge = ' · 🚀 **衝刺**'
    rank = r['today_rank']
    return (
        f'### `#{rank}` {r["code"]} {r["name"]}{badge}\n'
        f'> 🎯 蟄伏 `{r["score"]}` · 📈 動能 `{r["momentum"]:+d}` · '
        f'💰 `{r["close"]}元`\n'
        f'> 📉 離底 `+{r["low_zone_pct"]}%` · 📊 量 `{r["vol_ratio_3d"]}` · '
        f'⬆️ 昨 `{_arrow_tag(r)}`'
    )


def format_for_line(results, today):
    top = results[:TOP_N]
    sprint_n = sum(1 for r in top if _is_sprinter(r))
    date_str, wd = _date_header(today)

    p1 = [
        '🎯 蟄伏雷達 Top 20 [備援]',
        f'📅 {date_str} (週{wd})',
        f'全市場 {len(results)} 檔符合蟄伏條件',
        '',
        '『量縮整理、尚未啟動的底部股』',
        '歷史驗證 55% 會爆發',
        '平均 60 日最大漲幅 +29.5%',
        '',
        '━━━━━━━━━━━━━━',
        '💡 數字怎麼看',
        '━━━━━━━━━━━━━━',
        '🎯 蟄伏分 = 符合蟄伏的強度 (滿分17)',
        '📈 動能 = 排名上升速度',
        '         (越高代表剛被發現)',
        '💰 收盤價',
        '📉 離底 = 距60日最低價%',
        '📊 量  = 3日量÷20日量 (<1 量縮)',
        '⬆️ 昨 = 昨日排名變化',
        '',
        '🚀 衝刺型 (新進榜/快速上升)',
        '',
        '━━━━━━━━━━━━━━',
        '📊 Top 1-10',
        '━━━━━━━━━━━━━━',
        '',
    ]
    for r in top[:10]:
        p1.append(_line_stock_block(r))
        p1.append('')
    msg1 = '\n'.join(p1)

    p2 = [
        f'🎯 蟄伏雷達 Top 11-20 (續) [備援]',
        f'📅 {date_str}',
        '',
        '━━━━━━━━━━━━━━',
        '',
    ]
    for r in top[10:]:
        p2.append(_line_stock_block(r))
        p2.append('')
    p2.extend([
        '━━━━━━━━━━━━━━',
        f'本次 🚀 衝刺型 {sprint_n} 檔',
        '━━━━━━━━━━━━━━',
        '',
        '💡 動能越高 = 剛被發現的好機會',
        '   歷史 55% 會爆發',
        '',
        '⚠️ 此為 GitHub Actions 備援訊息',
    ])
    msg2 = '\n'.join(p2)
    return [msg1, msg2]


def format_for_discord(results, today):
    top = results[:TOP_N]
    sprint_n = sum(1 for r in top if _is_sprinter(r))
    date_str, wd = _date_header(today)

    p1 = [
        '# 🎯 蟄伏雷達 Top 20 `[備援]`',
        f'**📅 {date_str} (週{wd})** · 全市場 `{len(results)}` 檔',
        '',
        '> 『量縮整理、尚未啟動的底部股』',
        '> 歷史驗證 **55%** 爆發命中率',
        '> 平均 60 日最大漲幅 **+29.5%**',
        '',
        '━━━━━━━━━━━━━━━',
        '',
        '## 💡 指標說明',
        '> 🎯 **蟄伏分** (滿分 17) · 越高越符合蟄伏條件',
        '> 📈 **動能** · 排名上升速度 · 越高越剛被發現',
        '> 💰 **收盤價**',
        '> 📉 **離底** · 距 60 日最低價百分比',
        '> 📊 **量比** · 3日量÷20日量 · `<1` 代表量縮',
        '> ⬆️ **昨** · 昨日排名變化',
        '',
        '> 🚀 **衝刺型** (新進榜 或 快速上升)',
        '',
        '━━━━━━━━━━━━━━━',
        '',
        '## 📊 Top 1-10',
        '',
    ]
    for r in top[:10]:
        p1.append(_dc_stock_block(r))
        p1.append('')
    msg1 = '\n'.join(p1)

    p2 = [
        f'## 📊 Top 11-20 (續) · {date_str}',
        '',
    ]
    for r in top[10:]:
        p2.append(_dc_stock_block(r))
        p2.append('')
    p2.extend([
        '━━━━━━━━━━━━━━━',
        '',
        f'### 🚀 本次衝刺型 `{sprint_n}` 檔',
        '',
        '> 💡 **動能越高 = 剛被發現 = 最有爆發潛力**',
        '> 歷史 **55%** 命中率',
        '',
        '> ⚠️ 此為 GitHub Actions 備援訊息',
    ])
    msg2 = '\n'.join(p2)
    return [msg1, msg2]


# ── Push ──
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


def send_discord(text):
    webhook = os.environ.get('DISCORD_WEBHOOK_STOCK', '')
    if not webhook:
        return False
    # Split into chunks <=1990 chars at newlines
    chunks = []
    remaining = text
    while len(remaining) > 1990:
        split_at = remaining.rfind('\n', int(1990 * 0.6), 1990)
        if split_at == -1:
            split_at = 1990
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip('\n')
    if remaining:
        chunks.append(remaining)

    for chunk in chunks:
        payload = json.dumps({
            'content': chunk,
            'username': '蟄伏雷達 Bot [備援]',
        }).encode('utf-8')
        req = urllib.request.Request(
            webhook, data=payload, method='POST',
            headers={'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
                if r.status != 204:
                    print(f'Discord push unexpected status: {r.status}')
            time.sleep(0.4)
        except Exception as e:
            print(f'Discord push failed: {e}')
            return False
    return True


# ── History snapshot ──
def save_history(results, today):
    snapshot = {
        'date': today,
        'total_candidates': len(results),
        'top_20': results[:TOP_N],
    }
    with open(LATEST, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


# ── Main ──
def main():
    print('=== Taiwan Accumulation Radar (cloud backup) ===')
    print('Start:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    incremental_update()
    full_data = load_all_data_from_cache()
    print(f'Stocks loaded: {len(full_data)}')

    results, today = run_full_screen(full_data)
    if not results or not today:
        print('ERROR: no screening results')
        return 1
    print(f'Screened {len(results)} candidates for {today}')

    save_history(results, today)

    line_msgs = format_for_line(results, today)
    discord_msgs = format_for_discord(results, today)

    for i, m in enumerate(line_msgs, 1):
        print(f'\n--- LINE msg {i}/{len(line_msgs)} ({len(m)} chars) ---')
        print(m)

    token = os.environ.get('LINE_CHANNEL_TOKEN', '')
    user_id = os.environ.get('LINE_USER_ID', '')

    line_ok = True
    if not token or not user_id:
        print('ERROR: LINE_CHANNEL_TOKEN or LINE_USER_ID not set, skip LINE')
        line_ok = False
    else:
        for i, m in enumerate(line_msgs, 1):
            status, body = send_line(m, token, user_id)
            print(f'LINE msg {i}/{len(line_msgs)}: {status} {body[:120]}')
            if status != 200:
                line_ok = False
            time.sleep(0.6)

    for i, m in enumerate(discord_msgs, 1):
        print(f'Discord msg {i}/{len(discord_msgs)} ({len(m)} chars)')
        send_discord(m)
        time.sleep(0.6)

    return 0 if line_ok else 1


if __name__ == '__main__':
    sys.exit(main())
