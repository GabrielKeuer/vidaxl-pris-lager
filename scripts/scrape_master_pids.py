"""Scrape master_pid for vidaXL-SKUs — samtidig (ThreadPool) + streaming-stop + resumbar.

Henter feed (SKU→Link), streamer hver produktside og STOPPER så snart master_pid
('id: \"M...\"' i CQuotient-blokken ~261 KB inde) er fundet → downloader ~1/3 af siden.
Skriver løbende til JSONL-checkpoint → afbrydes den, springes færdige SKUs over.

Env: FEED_URL (påkrævet), SCRAPE_WORKERS (default 30), SCRAPE_LIMIT (0=alle),
     SCRAPE_MODE (pilot=tilfældigt udsnit af feed | full=SKUs fra SCRAPE_SKUS-fil),
     SCRAPE_SKUS (fil med vores SKUs, én pr. linje), SCRAPE_CKPT (checkpoint-sti).
"""
import csv, io, os, re, sys, time, json, zipfile, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

sys.stdout.reconfigure(encoding="utf-8")
_HUB = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_HUB):
    for l in open(_HUB, encoding="utf-8"):
        m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
        if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))

FEED_URL = os.environ["FEED_URL"]
WORKERS = int(os.environ.get("SCRAPE_WORKERS", "30"))
LIMIT = int(os.environ.get("SCRAPE_LIMIT", "0")) or None
MODE = os.environ.get("SCRAPE_MODE", "pilot")
SKUS_FILE = os.environ.get("SCRAPE_SKUS", "")
SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
CKPT = os.environ.get("SCRAPE_CKPT", os.path.join(SP, "master_pids.jsonl"))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
PID_RE = re.compile(rb"id:\s*'(M\d+)'|dwvar_(M\d+)|pid=(M\d+)")

def load_feed_links():
    print("📥 henter feed...")
    data = urllib.request.urlopen(FEED_URL, timeout=300).read()
    zf = zipfile.ZipFile(io.BytesIO(data))
    name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    sku_link = {}
    with zf.open(name) as f:
        rd = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in rd:
            s = (row.get("SKU") or "").strip().replace(".0", "")
            l = (row.get("Link") or "").strip()
            if s and l.startswith("http"):
                sku_link[s] = l
    print(f"   {len(sku_link)} SKU→Link i feed")
    return sku_link

def scrape_one(sku, url):
    t0 = time.time()
    try:
        r = requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=25)
        if r.status_code != 200:
            r.close(); return sku, None, f"http{r.status_code}", time.time() - t0, 0
        buf = b""
        for chunk in r.iter_content(32768):
            buf += chunk
            m = PID_RE.search(buf)
            if m:
                r.close()
                pid = next(g for g in m.groups() if g).decode()
                return sku, pid, "ok", time.time() - t0, len(buf)
            if len(buf) > 500000:  # pid ligger konsistent ~278 KB; cap godt over
                break
        r.close()
        # FALLBACK: pid ikke fundet i streamet del → hent HELE siden og søg igen
        # (fanger evt. sene/afvigende pids — vi misser aldrig et pid der findes)
        full = requests.get(url, headers={"User-Agent": UA}, timeout=25).content
        m = PID_RE.search(full)
        if m:
            pid = next(g for g in m.groups() if g).decode()
            return sku, pid, "ok_fallback", time.time() - t0, len(full)
        return sku, None, "no_pid", time.time() - t0, len(full)  # reelt uden master (single)
    except Exception as e:
        return sku, None, f"err:{type(e).__name__}", time.time() - t0, 0

def main():
    sku_link = load_feed_links()
    if MODE == "full" and SKUS_FILE and os.path.exists(SKUS_FILE):
        want = [s.strip().replace(".0", "") for s in open(SKUS_FILE) if s.strip()]
        todo = [(s, sku_link[s]) for s in want if s in sku_link]
        print(f"🎯 full: {len(want)} egne SKUs, {len(todo)} matcher feed")
    else:
        keys = list(sku_link.keys())
        step = max(1, len(keys) // (LIMIT or 500) // 3)
        todo = [(k, sku_link[k]) for k in keys[::step]]
        print(f"🔬 pilot: udsnit fra feed")
    if LIMIT:
        todo = todo[:LIMIT]

    done = set()
    if os.path.exists(CKPT):
        for l in open(CKPT, encoding="utf-8"):
            try: done.add(json.loads(l)["sku"])
            except Exception: pass
    todo = [(s, u) for s, u in todo if s not in done]
    print(f"⚙️  {len(todo)} at scrape ({len(done)} allerede done), {WORKERS} workers\n")

    ck = open(CKPT, "a", encoding="utf-8")
    n = ok = noblk = blk = 0; bytes_tot = 0; tstart = time.time(); status = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(scrape_one, s, u) for s, u in todo]
        for fut in as_completed(futs):
            sku, pid, st, dt, sz = fut.result()
            n += 1; bytes_tot += sz
            status[st.split(":")[0]] = status.get(st.split(":")[0], 0) + 1
            if pid:
                ok += 1; ck.write(json.dumps({"sku": sku, "master_pid": pid}) + "\n")
            if st.startswith("http4") or st.startswith("err"):
                blk += 1
            if n % 100 == 0:
                el = time.time() - tstart
                rate = n / el
                ck.flush()
                print(f"  {n}/{len(todo)} | {ok} pid | {rate:.1f}/s | "
                      f"{bytes_tot/1024/1024/max(el,1):.1f} MB/s | fejl/blok={blk} | ETA {((len(todo)-n)/max(rate,0.1))/60:.0f} min")
    ck.close()
    el = time.time() - tstart
    print(f"\n✅ {n} scrapet på {el/60:.1f} min | {ok} master_pid | {n/max(el,1):.1f}/s")
    print(f"   status: {status}")
    print(f"   distinkte masters: {len(set(json.loads(l)['master_pid'] for l in open(CKPT,encoding='utf-8')))}")
    if blk > n * 0.05:
        print(f"   ⚠️ BLOK-RATE {blk/max(n,1):.0%} — sænk WORKERS eller tilføj delay")
    else:
        print(f"   ✅ blok-rate lav ({blk/max(n,1):.1%}) — kan evt. skrue workers OP")

if __name__ == "__main__":
    main()
