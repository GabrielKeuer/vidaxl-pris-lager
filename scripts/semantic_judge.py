"""Semantisk LLM-dommer (READ-ONLY): validér at et udsnit af merges GIVER MENING.
Hører varianterne til samme produkt? Er den delte titel korrekt? Flagger tvivlsomme merges.
Bruger claude-sonnet-5. Sample spredt over størrelser."""
import json, os, re, sys, urllib.request
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
import merge_executor as ME
KEY = os.environ.get("ANTHROPIC_API_KEY")
SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 40

def call(prompt):
    body = {"model": "claude-sonnet-5", "max_tokens": 2500,
            "system": "Du er kvalitetskontrollør for en dansk møbelwebshop. Vurdér om produkt-konsolideringer "
                      "giver mening: hører varianterne til SAMME grundprodukt (blot forskellig farve/størrelse/"
                      "materiale)? Er den delte titel korrekt (ingen variant-specifik detalje)? Svar KUN med JSON: "
                      "[{\"nr\":N,\"verdict\":\"OK\"|\"PROBLEM\",\"reason\":\"kort\"}]",
            "messages": [{"role": "user", "content": prompt}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(),
                                 headers={"x-api-key": KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"})
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=120).read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read()[:200]}")
    txt = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
    if not txt:
        raise RuntimeError(f"tom: {str(r)[:160]}")
    m = re.search(r"\[.*\]", txt, re.S)
    return json.loads(m.group(0)) if m else []

def main():
    plans = [json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8")]
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    varz, prods = cache["vars"], cache["prods"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    pid2skus = defaultdict(list)
    for s, vv in varz.items():
        pid2skus[vv.get("pid")].append(s)
    ex = [p for p in plans if p["action"] in ("merge", "fix_mismerge_rest")
          and not p.get("unresolved_collisions") and not p.get("dup_sku_quarantine") and p["variant_creates"]]
    ex.sort(key=lambda p: len(p["variant_creates"]))
    step = max(1, len(ex) // N)
    sample = ex[::step][:N]

    items = []
    for i, p in enumerate(sample):
        master = p["key"].split("|")[1]
        skus = [m["sku"] for m in p["variant_creates"]]
        full = list(set(skus + pid2skus.get(h2pid.get(p["keeper_handle"]), [])))
        vals = defaultdict(set)
        for s in full:
            for k, v in ME.danish_opts(s, master).items():
                if v: vals[k].add(v)
        axes = {k: sorted(v)[:6] for k, v in vals.items() if len(v) > 1}
        items.append({"nr": i, "titel": p["new_title"], "akser": axes,
                      "tilføjes": len(skus), "sletter_produkter": [d["handle"] for d in p["product_deletes"]][:3]})

    problems = []
    for i in range(0, len(items), 8):
        batch = items[i:i + 8]
        prompt = "Vurdér disse konsolideringer:\n" + json.dumps(batch, ensure_ascii=False, indent=1)
        try:
            for v in call(prompt):
                if v.get("verdict") == "PROBLEM":
                    it = items[v["nr"]]
                    problems.append((it["titel"], it["akser"], v.get("reason")))
        except Exception as e:
            print(f"  batch-fejl: {e}")
    print(f"\n=== SEMANTISK DOM: {len(sample)} merges vurderet ===")
    if not problems:
        print("✅ ingen semantiske problemer fundet i udsnittet")
    else:
        print(f"⚠ {len(problems)} flagget:")
        for t, a, r in problems:
            print(f"  • '{t}' {list(a)} → {r}")

if __name__ == "__main__":
    main()
