"""REPARÉR no-op keepers rørt af batch 1 (fejlagtigt processeret før no-op-skip-fixet).
Gendanner titel fra cache (batch strippede identitet) + fjerner bogus dublet-akser ('Størrelse 2'
osv. + '—'-pladsholdere) ved at kollapse til én værdi + slette. Priser/metafelter røres ikke
(daglig repricing er autoritativ; sku-only-1.-variant er konventionen). --live for at udføre."""
import json, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

SP = r"C:\Users\APC\AppData\Local\Temp\claude\C--Users-APC\c0b60326-0d7f-46aa-bec2-7289b435d558\scratchpad"

def main():
    live = "--live" in sys.argv
    plans = {p["key"]: p for p in (json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8"))}
    cache = json.load(open(SP + r"\plan_data_cache.json", encoding="utf-8"))
    prods, varz = cache["prods"], cache["vars"]
    h2pid = {pr["handle"]: pid for pid, pr in prods.items() if isinstance(pr, dict) and pr.get("handle")}
    # CACHE-drevet sandhed pr. produkt: {akse: sæt af værdier} (pre-batch). Batch-tilføjede akser =
    # akser IKKE i cachen hvis værdier heller IKKE matcher en cache-akse der mangler live (= omdøbning).
    from collections import defaultdict
    cache_av = defaultdict(lambda: defaultdict(set))
    for s, vv in varz.items():
        if vv.get("pid"):
            for a, val in (vv.get("opts") or {}).items():
                if val:
                    cache_av[vv["pid"]][a].add(val.lower())
    # Repair-mål: (1) no-op keepers rørt af batch 1 (fra log), (2) FAILED-grupper (delvis merge —
    # keeper fik batch-akse men bulkCreate fejlede). Begge = ufuldstændige → gendan til cache-tilstand.
    sb = ME.get_supabase_client()
    noop = set()
    for lg in ("merge_batch1.log", "merge_batch2.log"):
        try:
            for line in open(SP + "\\" + lg, encoding="utf-8", errors="ignore"):
                m = re.match(r"▶ (\S+) \[", line.strip())
                if m and plans.get(m.group(1)) and not plans[m.group(1)]["variant_creates"] and not plans[m.group(1)]["product_deletes"]:
                    noop.add(plans[m.group(1)]["keeper_handle"])
        except FileNotFoundError:
            pass
    for r in (sb.table("merge_exec_log").select("group_key").eq("status", "failed").execute().data or []):
        if plans.get(r["group_key"]):
            noop.add(plans[r["group_key"]]["keeper_handle"])
    # SIKKERHED: ekskludér keepers der er i en DONE-gruppe (succesfuld merge — deres nye akser er ægte!)
    done_handles = {plans[r["group_key"]]["keeper_handle"]
                    for r in (sb.table("merge_exec_log").select("group_key").eq("status", "done").execute().data or [])
                    if plans.get(r["group_key"])}
    noop = sorted(noop - done_handles)   # liste af keeper-handles der IKKE er succesfuldt merged
    print(f"{'LIVE' if live else 'DRY-RUN'}: {len(noop)} keepers (no-op + failed) at reparere\n")
    t_fix = axis_fix = flagged = 0
    for h in noop:
        cache_title = (prods.get(h2pid.get(h), {}) or {}).get("title")
        d = ME.gql('query{productByHandle(handle:$h){id title options{id name optionValues{name}} variants(first:100){edges{node{id}}}}}'.replace("$h", '"%s"' % h))
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            continue
        pid = pr["id"]
        cav = cache_av.get(h2pid.get(h), {})            # {cache_akse: sæt(lower-værdier)}
        cax = set(cav)
        live_names = {o["name"] for o in pr["options"] if o["name"] != "Title"}
        n_extra = len(live_names) - len(cax)
        # 1) gendan titel — KUN hvis batchen ændrede INDHOLDET (droppede/ændrede ord), ikke ren casing
        # (batchens '100x40x40' er bedre end cachens '100X40X40' → rør ikke rene casing-diffs)
        if cache_title and pr["title"].strip().lower() != cache_title.strip().lower():
            t_fix += 1
            print(f"  titel: {h[:45]}  '{pr['title']}' → '{cache_title}'")
            if live:
                ME.gql("mutation($i:ProductInput!){productUpdate(input:$i){userErrors{message}}}",
                       {"i": {"id": pid, "title": cache_title, "seo": {"title": cache_title[:70]}}})
        # 2) fjern BATCH-TILFØJEDE akser (root-cause: gendan til cachens akse-sæt). En live-akse fjernes
        #    hvis navn IKKE i cache OG dens værdier IKKE matcher en cache-akse der mangler live (omdøbning).
        #    → beholder ægte cache-akser + rene omdøbninger; fjerner ægte batch-tilføjede (dubletter OG nye).
        bogus = []
        if n_extra > 0 and cax:
            absent = {a: vals for a, vals in cav.items() if a not in live_names}   # omdøbt-væk cache-akser
            for o in pr["options"]:
                if o["name"] == "Title" or o["name"] in cax:
                    continue
                ov = {v["name"].lower() for v in o["optionValues"] if v["name"] != "—"}
                # omdøbning? værdier matcher en fraværende cache-akse (kraftig overlap)
                is_rename = any(ov & vals and len(ov & vals) >= 0.5 * len(ov) for vals in absent.values()) if ov else False
                if not is_rename:
                    bogus.append(o)
            bogus = bogus[:max(0, n_extra)]
        for o in bogus:
            axis_fix += 1
            print(f"  akse:  {h[:45]}  fjerner '{o['name']}'")
            if live:
                vids = [e["node"]["id"] for e in pr["variants"]["edges"]]
                ups = [{"id": vid, "optionValues": [{"optionName": o["name"], "name": "Standard"}]} for vid in vids]
                for i in range(0, len(ups), 100):
                    ME.gql("mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){productVariantsBulkUpdate(productId:$pid,variants:$v){userErrors{message}}}",
                           {"pid": pid, "v": ups[i:i+100]})
                r = ME.gql("mutation($pid:ID!,$o:[ID!]!){productOptionsDelete(productId:$pid,options:$o,strategy:DEFAULT){userErrors{field message}}}",
                           {"pid": pid, "o": [o["id"]]})
                errs = r["data"]["productOptionsDelete"]["userErrors"]
                if errs:
                    flagged += 1; print(f"     ⚠ slet-fejl: {errs}")
    print(f"\n{'UDFØRT' if live else 'VILLE GØRE'}: {t_fix} titel-gendan, {axis_fix} bogus-akser fjernet, {flagged} flag")

if __name__ == "__main__":
    main()
