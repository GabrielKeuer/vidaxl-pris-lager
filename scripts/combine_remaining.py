"""Printer '<tilbage> <gjort>' for combine-planen (respekterer recent_masters-guard). Bruges af
combine_execute-workflowen til at afgøre om vi er FÆRDIGE (0 tilbage → besked + auto-stop)."""
import json, os
plan = json.load(open("output/combine_plan.json", encoding="utf-8"))
rec = set(json.load(open("output/recent_masters.json", encoding="utf-8"))) if os.path.exists("output/recent_masters.json") else set()
done = set(json.load(open("output/combine_done.json", encoding="utf-8"))) if os.path.exists("output/combine_done.json") else set()
todo = [c for c in plan if c["mid"] not in rec and c["mid"] + "|" + c["title"] not in done]
print(f"{len(todo)} {len(done)}")
