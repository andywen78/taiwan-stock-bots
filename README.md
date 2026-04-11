# taiwan-bullish-filter

Cloud-runnable Taiwan stock weekly bullish-alignment + volume-surge filter.

Runs in an ephemeral environment (Claude Code remote trigger / CCR) every trading day at 19:00 Asia/Taipei, scans TWSE + TPEX, and pushes a ranked list to LINE.

## Filter conditions

1. Weekly MA5 > MA10 > MA20 (bullish alignment holds for the past month)
2. Weekly volume surge ≥ 1.5× prior 5-week average (at least once in past month)
3. Daily close ≥ 20MA × 0.98
4. Daily close never below 60MA
5. 5-day avg volume ≥ 1000 lots

## Top-of-top

Filtered list is further narrowed by: surge ratio ≥ 3.0, min20 ≥ 1.0, min60 ≥ 1.05. Top 15 by surge ratio.

## Run

```
LINE_CHANNEL_TOKEN=xxx LINE_USER_ID=yyy python3 run_filter.py
```
