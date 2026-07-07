"""Beregn faktisk variant-oprettelses-volumen for merge (keeper=størst, opret kun delta)
+ opgør variant-fejlgrupperinger. READ-ONLY."""
import json, os, re, sys
from collections import defaultdict, Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
for l in open(r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local", encoding="utf-8"):
    m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", l)
    if m: os.environ.setdefault(m.group(1), m.group(2).strip().strip('"').strip("'"))
from analyze_master_mapping import load_mapping, export_products

mapping = load_mapping()
prods = export_products()

# master -> {pid: antal SKUs af den master i det produkt}
master_pid_counts = defaultdict(Counter)
for pid, p in prods.items():
    for s in p["skus"]:
        mp = mapping.get(s)
        if mp: master_pid_counts[mp][pid] += 1

create_delta = 0          # varianter vi SKAL oprette på keepere
groups_to_merge = 0       # antal master-grupper der merges
products_removed = 0      # gamle produkter der slettes/redirectes efter merge
big_after = 0             # merged produkter der ender >100 varianter
for mp, pc in master_pid_counts.items():
    if len(pc) < 2:
        continue  # kun manglende-merge-grupper
    groups_to_merge += 1
    total = sum(pc.values())
    keeper = max(pc.values())
    create_delta += (total - keeper)
    products_removed += (len(pc) - 1)
    if total > 100:
        big_after += 1

# fejl-merges: varianter der ligger i forkert produkt (SKUs hvis master ikke er produktets flertals-master)
misplaced = 0
mismerge_products = 0
for pid, p in prods.items():
    masters = [mapping[s] for s in p["skus"] if mapping.get(s)]
    if len(set(masters)) > 1:
        mismerge_products += 1
        dom = Counter(masters).most_common(1)[0][0]
        misplaced += sum(1 for x in masters if x != dom)

print("=" * 60)
print("MERGE-VOLUMEN (keeper = produkt m. flest af master'ens varianter)")
print(f"  master-grupper der merges     : {groups_to_merge}")
print(f"  gamle produkter der fjernes    : {products_removed}")
print(f"  VARIANTER at OPRETTE (delta)   : {create_delta}")
print(f"  → ved ~1000/dag: ~{create_delta/1000:.0f} dage batchet")
print(f"  merged produkter >100 varianter: {big_after} (large-product-flow)")
print()
print("FEJL-GRUPPERINGER (varianter i forkert produkt)")
print(f"  produkter med blandede masters : {mismerge_products}")
print(f"  fejlplacerede varianter i alt  : {misplaced}")
