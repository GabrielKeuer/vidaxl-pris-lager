"""Sikre at de allerede-merged produkter (status=done) matcher den NYE autoritative logik: fjern
DØDE legacy-options (1 værdi) der ikke er i den autoritative item_variant-struktur. Flag multi-værdi-
legacy-konflikter (kræver rename/rebuild). --live for at udføre. READ-ONLY uden --live."""
import json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
import merge_executor as ME

def main():
    live = "--live" in sys.argv
    plans = {p["key"]: p for p in (json.loads(l) for l in open("output/merge_plan.jsonl", encoding="utf-8"))}
    sb = ME.get_supabase_client()
    done = [r["group_key"] for r in sb.table("merge_exec_log").select("group_key").eq("status", "done").execute().data or []]
    print(f"{'LIVE' if live else 'DRY-RUN'}: tjekker {len(done)} merged produkter for legacy-options\n")
    removed = flagged = clean = 0
    for k in done:
        p = plans.get(k)
        if not p:
            continue
        master = k.split("|")[1] if "|" in k else ""
        h = p["keeper_handle"]
        d = ME.gql("""query($h:String!){productByHandle(handle:$h){id options{id name optionValues{name}}
          variants(first:250){edges{node{id sku}}}}}""", {"h": h})
        pr = (d.get("data") or {}).get("productByHandle")
        if not pr:
            continue
        skus = [(e["node"]["sku"] or "").strip() for e in pr["variants"]["edges"]]
        km = ME.build_keyname(skus, master)
        av = defaultdict(set)
        for s in skus:
            for a, v in ME.danish_opts(s, master, km).items():
                if v: av[a].add(v)
        target = {a for a, vv in av.items() if len(vv) > 1}
        legacy = [o for o in pr["options"] if o["name"] != "Title" and o["name"] not in target]
        dead = [o for o in legacy if len(o["optionValues"]) <= 1]
        multi = [o for o in legacy if len(o["optionValues"]) > 1]
        if multi:
            flagged += 1
            print(f"  ⚠ {h[:48]}: multi-værdi legacy {[o['name'] for o in multi]} (target={sorted(target)}) — kræver rename")
        if dead:
            removed += 1
            print(f"  🧹 {h[:48]}: fjerner død legacy {[o['name'] for o in dead]}")
            if live:
                vids = [e["node"]["id"] for e in pr["variants"]["edges"]]
                for o in dead:
                    ups = [{"id": vid, "optionValues": [{"optionName": o["name"], "name": "Standard"}]} for vid in vids]
                    for i in range(0, len(ups), 100):
                        ME.gql("mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){productVariantsBulkUpdate(productId:$pid,variants:$v){userErrors{message}}}", {"pid": pr["id"], "v": ups[i:i+100]})
                    ME.gql("mutation($pid:ID!,$o:[ID!]!){productOptionsDelete(productId:$pid,options:$o,strategy:DEFAULT){userErrors{message}}}", {"pid": pr["id"], "o": [o["id"]]})
        if not dead and not multi:
            clean += 1
    print(f"\n{'UDFØRT' if live else 'VILLE'}: {clean} allerede rene, {removed} m. død legacy fjernet, {flagged} flagget (rename)")

if __name__ == "__main__":
    main()
