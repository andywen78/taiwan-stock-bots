#!/usr/bin/env python3
"""Taiwan Stock weekly bullish filter + LINE push notification.

Cloud-compatible version. Runs in ephemeral CCR environment.
Reads LINE_CHANNEL_TOKEN and LINE_USER_ID from environment.
"""
import os, sys, json, ssl, time, urllib.request, urllib.error
from datetime import datetime, date, timedelta

sys.stdout.reconfigure(encoding='utf-8')

CACHE_DIR = '/tmp/bulk_cache'
ALL_DAILY = '/tmp/all_daily.json'
HISTORY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'history')
LATEST_TOP = HISTORY_DIR + '/latest.json'
SKIP_DATE = '20260223'
TOLERANCE = 0.98
MIN_VOL_LOTS = 1000
FETCH_DAYS = 220  # ~31 weeks, matches local behavior and handles late-2025 IPOs

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

def tick_size(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def limit_up_price(prev_close):
    import math
    raw = prev_close * 1.10
    t = tick_size(raw)
    return round(math.floor(raw / t + 1e-9) * t, 4)

def limit_down_price(prev_close):
    import math
    raw = prev_close * 0.90
    t = tick_size(raw)
    return round(math.ceil(raw / t - 1e-9) * t, 4)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def load_line_config():
    return os.environ.get('LINE_CHANNEL_TOKEN'), os.environ.get('LINE_USER_ID')

def fetch_twse_day(d):
    cache = CACHE_DIR + '/twse_' + d + '.json'
    if os.path.exists(cache):
        return 'cached'
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=' + d + '&type=ALL'
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
        return 'ERR:' + str(e)

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
            tw = fetch_twse_day(ds)
            if tw != 'cached':
                time.sleep(1.5)
                fetched += 1
            tp = fetch_tpex_day(ds)
            if tp != 'cached':
                time.sleep(0.5)
                fetched += 1
        d += timedelta(days=1)
    print(f'Incremental fetch: {fetched} new API calls')

def rebuild_all_daily():
    all_data = {}
    for fn in sorted(os.listdir(CACHE_DIR)):
        d = fn.replace('.json', '').split('_')[1]
        with open(CACHE_DIR + '/' + fn, encoding='utf-8') as f:
            rows = json.load(f)
        for row in rows:
            code, name, op, hi, lo, cl, vol = row
            if code not in all_data:
                all_data[code] = {'name': name, 'bars': []}
            all_data[code]['bars'].append([d, op, hi, lo, cl, vol])
    for code in all_data:
        seen = {}
        for b in all_data[code]['bars']:
            seen[b[0]] = b
        all_data[code]['bars'] = sorted(seen.values(), key=lambda x: x[0])
    with open(ALL_DAILY, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False)
    return all_data

def to_weekly(bars):
    weekly = []
    cw = None
    cur = None
    for b in bars:
        dt = datetime.strptime(b[0], '%Y%m%d')
        y, w, _ = dt.isocalendar()
        key = (y, w)
        if key != cw:
            if cur:
                weekly.append(cur)
            cw = key
            cur = {'dates': [b[0]], 'cl': b[4], 'vol': b[5]}
        else:
            cur['dates'].append(b[0])
            cur['cl'] = b[4]
            cur['vol'] += b[5]
    if cur:
        weekly.append(cur)
    return weekly

def run_filter(all_data):
    today = date.today()
    start = (today - timedelta(days=30)).strftime('%Y%m%d')

    qualified = []
    for code, info in all_data.items():
        bars = [b for b in info['bars'] if b[0] != SKIP_DATE]
        bars.sort(key=lambda x: x[0])
        if len(bars) < 100:
            continue
        weekly = to_weekly(bars)
        if len(weekly) < 22:
            continue

        closes_w = [w['cl'] for w in weekly]
        vols_w = [w['vol'] for w in weekly]

        recent_weeks = [i for i, w in enumerate(weekly) if w['dates'][-1] >= start]
        if not recent_weeks:
            continue

        aligned_all = True
        for idx in recent_weeks:
            if idx < 19:
                aligned_all = False
                break
            m5 = sum(closes_w[idx - 4:idx + 1]) / 5
            m10 = sum(closes_w[idx - 9:idx + 1]) / 10
            m20 = sum(closes_w[idx - 19:idx + 1]) / 20
            if not (m5 > m10 > m20):
                aligned_all = False
                break
        if not aligned_all:
            continue

        surge_found = False
        max_surge_ratio = 0
        surge_week = None
        for idx in recent_weeks:
            if idx < 5:
                continue
            prior = vols_w[idx - 5:idx]
            avg = sum(prior) / len(prior) if prior else 0
            if avg == 0:
                continue
            ratio = vols_w[idx] / avg
            if ratio > max_surge_ratio:
                max_surge_ratio = ratio
                surge_week = weekly[idx]['dates'][-1]
            if ratio >= 1.5:
                surge_found = True
        if not surge_found:
            continue

        daily_opens = [b[1] for b in bars]
        daily_closes = [b[4] for b in bars]
        daily_dates = [b[0] for b in bars]
        daily_vols = [b[5] for b in bars]

        broke20 = False
        min20 = None
        for i in range(len(bars)):
            if i < 19 or daily_dates[i] < start:
                continue
            dma20 = sum(daily_closes[i - 19:i + 1]) / 20
            ratio = daily_closes[i] / dma20
            if min20 is None or ratio < min20:
                min20 = ratio
            if ratio < TOLERANCE:
                broke20 = True
                break
        if broke20:
            continue

        broke60 = False
        min60 = None
        for i in range(len(bars)):
            if i < 59 or daily_dates[i] < start:
                continue
            dma60 = sum(daily_closes[i - 59:i + 1]) / 60
            ratio60 = daily_closes[i] / dma60
            if min60 is None or ratio60 < min60:
                min60 = ratio60
            if daily_closes[i] < dma60:
                broke60 = True
                break
        if broke60:
            continue

        avg5_lots = (sum(daily_vols[-5:]) / 5) / 1000
        if avg5_lots < MIN_VOL_LOTS:
            continue

        limit = None
        if len(daily_closes) >= 2:
            prev_close = daily_closes[-2]
            if prev_close > 0:
                lu = limit_up_price(prev_close)
                ld = limit_down_price(prev_close)
                if abs(daily_closes[-1] - lu) < 1e-4:
                    limit = 'up'
                elif abs(daily_closes[-1] - ld) < 1e-4:
                    limit = 'down'

        qualified.append({
            'code': code,
            'name': info['name'],
            'last_open': daily_opens[-1],
            'last_close': daily_closes[-1],
            'limit': limit,
            'surge_ratio': max_surge_ratio,
            'surge_week': surge_week,
            'min20': min20,
            'min60': min60,
            'avg5_lots': avg5_lots,
        })

    qualified.sort(key=lambda x: -x['surge_ratio'])
    return qualified

def pick_top_of_top(qualified, n=15):
    top = [s for s in qualified
           if s['surge_ratio'] >= 3.0 and s['min20'] >= 1.0 and s['min60'] >= 1.05]
    return top[:n]

def save_history(top, date_str):
    snapshot = {
        'date': date_str,
        'top': [{'code': s['code'], 'name': s['name'],
                 'surge_ratio': round(s['surge_ratio'], 2),
                 'last_close': round(s['last_close'], 2)} for s in top]
    }
    with open(LATEST_TOP, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    return snapshot

def load_previous():
    if not os.path.exists(LATEST_TOP):
        return None
    try:
        with open(LATEST_TOP, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def compute_diff(prev_top, today_top):
    prev_codes = {s['code']: s for s in prev_top} if prev_top else {}
    today_codes = {s['code']: s for s in today_top}
    added = [today_codes[c] for c in today_codes if c not in prev_codes]
    removed = [prev_codes[c] for c in prev_codes if c not in today_codes]
    return added, removed

def format_diff_message(added, removed, today_str, prev_date):
    lines = ['\U0001f514 台股強中之強異動 ' + today_str, '']
    lines.append('（對照上次：' + prev_date + '）')
    lines.append('')
    if added:
        lines.append('➕ 新入選 ' + str(len(added)) + ' 檔：')
        for s in added:
            lines.append(f"  {s['code']} {s['name']}  量比 {s['surge_ratio']:.2f}x")
    if removed:
        lines.append('')
        lines.append('➖ 移出 ' + str(len(removed)) + ' 檔：')
        for s in removed:
            lines.append(f"  {s['code']} {s['name']}  (前 {s['surge_ratio']:.2f}x)")
    if not added and not removed:
        lines.append('✅ 名單無異動')
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

def format_message(top, qualified_count, date_str):
    lines = ['📈 台股強中之強 ' + date_str, '']
    lines.append('週線多頭排列 + 爆量')
    lines.append('站穩 20MA/60MA（近1個月）')
    lines.append('')
    if not top:
        lines.append('⚠️ 今日無符合「強中之強」的個股')
    else:
        lines.append('🏆 精選 ' + str(len(top)) + ' 檔')
        for i, s in enumerate(top, 1):
            lim = ''
            if s.get('limit') == 'up':
                lim = ' (漲停)'
            elif s.get('limit') == 'down':
                lim = ' (跌停)'
            lines.append(f"{i}. {s['code']} {s['name']}  量比 {s['surge_ratio']:.2f}x")
            lines.append(f"   開 {s['last_open']:.2f}  收 {s['last_close']:.2f}{lim}")
    lines.append('')
    lines.append('全部符合5條件：' + str(qualified_count) + ' 檔')
    return '\n'.join(lines)

def main():
    print('=== Taiwan Bullish Filter (cloud) ===')
    print('Start:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    incremental_update()
    all_data = rebuild_all_daily()
    print('Stocks loaded:', len(all_data))

    qualified = run_filter(all_data)
    print('Qualified (5 conditions):', len(qualified))

    top = pick_top_of_top(qualified)
    print('Top of top:', len(top))
    for s in top:
        print(f"  {s['code']} {s['name']} {s['surge_ratio']:.2f}x min20={s['min20']:.3f}")

    today_str = date.today().strftime('%Y-%m-%d')
    msg = format_message(top, len(qualified), today_str)
    print('\n--- LINE message ---')
    print(msg)

    token, uid = load_line_config()
    if not token or not uid:
        print('ERROR: LINE_CHANNEL_TOKEN or LINE_USER_ID not set in environment')
        return 1

    # Load previous snapshot BEFORE overwriting
    prev = load_previous()
    prev_top = prev['top'] if prev else None
    prev_date = prev['date'] if prev else None

    # Save today's snapshot (will be committed back to repo by workflow)
    save_history(top, today_str)

    status, body = send_line(msg, token, uid)
    print(f'\nLINE push #1 (daily) status={status} body={body[:150]}')

    # Send diff notification if there are changes
    if prev_top is not None and prev_date != today_str:
        added, removed = compute_diff(prev_top, top)
        if added or removed:
            diff_msg = format_diff_message(added, removed, today_str, prev_date)
            print('\n--- Diff message ---')
            print(diff_msg)
            status2, body2 = send_line(diff_msg, token, uid)
            print(f'\nLINE push #2 (diff) status={status2} body={body2[:150]}')
        else:
            print('\nNo changes vs ' + prev_date + ', skip diff notification')
    else:
        print('\nNo previous snapshot or same date, skip diff notification')

    return 0 if status == 200 else 1

if __name__ == '__main__':
    sys.exit(main())
