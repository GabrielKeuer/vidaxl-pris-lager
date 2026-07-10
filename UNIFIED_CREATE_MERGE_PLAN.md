# Samlet Create + Merge-flow — design & byggeplan

**Mål:** Ét dagligt flow der producerer det korrekte sæt Shopify-produkter fra vidaXL-feedet med ÉN
grupperings-logik — opretter nye produkter OG opdaterer/merger eksisterende konsistent. Erstatter de to
adskilte flows (dropxl `daily_create`/`daily_create_large` + vidaxl-pris-lager `combine_execute`).

**Status:** DESIGN + Fase 1 påbegyndt. Intet live ændres før dry-run + lille live-test er grønne. Combine-backlog
(~664/1218) kører færdig først (den er bevist); det samlede flow overtager derefter både create og merge.

---

## 🔖 GENOPTAG HER (multi-session — læs FØRST)

**Bygget indtil nu (2026-07-10):**
- ✅ `scripts/unified_sync.py` — Fase 1-DRIVER: én gruppering over hele feedet + change-detektion via live-snapshot
  (sku2pid) + klassificering (CREATE / MERGE_nyvariant / MERGE_konsolidér / UNCHANGED / PARK_split / SKIP_lavt_lager).
  **KUN klassificering/DRY-RUN — ingen apply endnu.** Kør: `python scripts/unified_sync.py [--refresh] [--limit N] [--only MID]`.
  Output: `output/unified_dryrun.json`. Verificeret på 200 masters (kører).

**MANGLER at bygge (næste sessioner, i rækkefølge):**
1. **Fase 0 — `scripts/catalog_engine.py`:** udtræk combine_exec's per-gruppe-kerne til en delt
   `process_group(group, create_if_missing=True, dry_run=...)`. **RØR IKKE `combine_exec.py`** mens combine-cron'en
   kører live (backlog ~561 tilbage) — byg catalog_engine som NY fil; migrér combine_exec til at bruge den FØRST
   når backlog er færdig + catalog_engine er testet. process_group skal kunne: (a) OPRET nyt (productSet uden
   product_id + publicér) når ingen live-fragmenter, (b) MERGE in-place (som combine_exec i dag) når fragmenter
   findes. Genbrug merge_anchor/dedup/create_single/chunked/reorder/handle/redirect fra combine_exec (kopiér ind).
2. **Fase 1b — wire `unified_sync --live` → process_group** + tilføj DELTA-change-detektion (state/last_catalog_skus.csv
   snapshot: kun masters m. tilføjede/manglende feed-SKU processeres; unified_sync klassificerer i dag ALLE masters).
3. **Fase 1c — create-filtre-paritet:** i dag kun MIN_STOCK_PRIMARY=20 approksimeret; tilføj MIN_STOCK_VARIANT=4 +
   aktiv-hovedkategori-tjek (som daily_create).
4. **Fase 2 — fuld dry-run** (frisk snapshot, EFTER combine-backlog er færdig — ellers moving target) → diff mod
   Shopify + mod hvad daily_create ville lave → dokumentér ønskede vs uønskede afvigelser.
5. **Fase 3 — lille live-test** (5-10 masters: nye + ny-variant-til-eksisterende + dublet) → slavisk validering
   (SKU-sæt/titel/1.variant/metafelter/stocked/pris/handle/residual-match).
6. **Fase 4 — cutover:** `unified_execute.yml`-workflow (vidaxl-pris-lager, cross-repo DROPXL_PAT + SHOPIFY_COMBINE_TOKEN
   + SUPABASE + FEED_URL); disable dropxl `daily_create`+`daily_create_large`; behold daily_delete + sync_inventory + repricing.

**KRITISKE FAKTA (må ikke glemmes):**
- Combine-cron (`combine_execute.yml`) er LIVE (~664/1218). `combine_exec.py` MÅ IKKE brydes → byg unified som separate filer.
- Unified bor i vidaxl-pris-lager, importerer create_products_v2/product_utils fra dropxl via `DROPXL_SCRIPTS`-env (cross-repo).
- Token: `SHOPIFY_COMBINE_TOKEN` (write_products+write_content) til create+merge+redirects. Almindeligt SHOPIFY_ACCESS_TOKEN
  mangler write_content. Lager-aktivering sker under write_products (bevist).
- `PARK_split` i dry-run = split-backloggen (eksisterende produkter der ikke matcher unified-grupperingen → parkeres, vi splitter ikke).
- INTET live før Fase 3 grøn.

---

## 1. Problemet vi løser

I dag grupperer de to flows FORSKELLIGT:
- **combine** (eksisterende produkter): master_pid + strip variant-options → gruppér på residual + fuzzy
  (ental/flertal) + mørk-farve. **Splitter en master i flere produkter** når residualet er forskelligt.
- **create** (nye produkter): master_pid → ALLE søskende = ÉT produkt. Ingen residual-split/fuzzy/mørk.

Konsekvens hvis create tændes uændret: (a) nye masters kan blive fejl-grupperet (Kontinentalseng + Box Spring
i ét), (b) en ny variant til en allerede-splittet master havner på det FORKERTE af de flere produkter, fordi
create kun kender "masteren findes" — ikke hvilket residual-produkt den nye SKU hører til.

**Løsning:** samme grupperings-kerne bruges af create og merge → nye varianter havner altid rigtigt.

---

## 2. Målarkitektur — kerne-loop

For hver **mål-gruppe** (fra grupperingen af hele feedet):

```
group = {master_pid, residual-titel, akser, skus[]}
frag  = live-produkter der holder gruppens SKU        (old_products_for_skus)
spec  = build_spec(group)                             (fuld opskrift: titel, varianter, metafelter, pris, lager)
if frag:      # produktet findes → MERGE/OPDATÉR
    anchor = reneste handle blandt frag
    productSet(spec, product_id=anchor)  + evt. bulk-create (chunked >250)
    + rent handle/301 + reorder + redirect&slet donorer   (combine_exec-adfærd)
else:         # produktet findes ikke → OPRET NYT
    productSet(spec)                     + evt. bulk-create (chunked >250)
    + publicér
begge:  dedup identiske option-kombos + create_single-sikkerhedsnet
```

Fordi `spec` genopbygges fra HELE gruppen hver gang, håndteres "eksisterende produkt + ny variant" automatisk:
den nye SKU er med i gruppen → med i spec → titlen regenereres (akse 1→2 værdier → farve ud af titel) → produktet
opdateres in-place. Se §7.

---

## 3. Change-detektion (så vi IKKE genopbygger 18k produkter dagligt)

Fuld daglig genopbygning er infeasible. Flowet er **delta-baseret** — rører kun grupper der har ændret sig:

- **Ny SKU** (create-kandidat): i feed, IKKE i Shopify, opfylder filtre (Stock ≥ 20 primær / ≥ 4 variant,
  B2B-pris > 0, aktiv hovedkategori — som create i dag).
- **Ny variant til eksisterende**: en master hvor feedets SKU-sæt er VOKSET siden sidste kørsel.
- **Udgået SKU**: i Shopify, ikke længere i feed → håndteres af `daily_delete` (uændret) ELLER integreres senere.

Kilde: sammenlign feedets SKU-pr-master mod en **sidste-kendt snapshot** (`state/last_catalog_skus.csv`, samme
mønster som `state/last_inventory.csv`). Kun masters med tilføjede/manglende SKU (eller helt-nye) grupperes +
processeres. Uændrede masters springes over. Snapshot committes efter hver kørsel.

Lager/pris rører flowet IKKE løbende — det gør `sync_inventory` + repricing (uændret). Dette flow ejer KUN
STRUKTUR (produkter/varianter/titler/handles/metafelter).

---

## 4. Genbrug — næsten alt findes allerede (testet)

| Komponent | Fil (vidaxl-pris-lager) | Rolle i samlet flow |
|---|---|---|
| Gruppering (residual+fuzzy+mørk) | `regroup.py`, `fix_live.regroup_master` | mål-grupper pr. master |
| Farve/mål-strip + universer | `build_complete_feed.py`, `scope_split.py` | residual + akse-værdier |
| Byg fuld opskrift | `cleanup_engine.build_spec` + `to_product_spec` | titel/varianter/metafelter/pris/lager |
| Apply (create-or-update, chunked, dedup, handles, redirects, reorder, create_single) | `combine_exec.py` (kernen) | selve mutationen |
| productSet/bulk-create/publish/handle/pris | `create_products_v2.py` (dropxl, via `DROPXL_SCRIPTS`) | lav-niveau Shopify-kald |

**Nyt der skal bygges:**
1. **Feed-driver** i stedet for `combine_plan.json`: iterér alle (eller kun ændrede) masters → regroup → grupper.
2. **Create-ny-gren**: når `frag` er tom → `productSet` UDEN product_id (opret) + publicér. (combine_exec springer
   i dag "ingen fragmenter" over — skal i stedet oprette.)
3. **Change-detektion** (§3) + snapshot-fil.
4. **Kandidat-filtre** for create (stock/pris/kategori); merge/opdatér altid.
5. Refaktorér combine_exec's kerne-loop til en genbrugelig `process_group(group)` som både combine-backlog og
   det nye daglige flow kalder.

---

## 5. Gaps der lukkes (fra overblikket)

| Gap | Lukkes |
|---|---|
| Gruppering (residual/fuzzy/mørk) forskellig | ÉN delt `regroup`-kerne begge veje |
| Dublet-variant: create fejler på identiske kombos | combine's dedup + `create_single` gælder også create |
| Kolonne-orden Farve→Form vs Farve→Størrelse | ÉN regel: Farve → Størrelse(SIZE_AXES) → resten (combine-standard) |
| Store produkter: 200/dag-drip vs chunked | combine's chunked (productSet 250 + bulk-create) overalt — strømlinet |

---

## 6. Hvor koden bor (undgå duplikering/drift)

Det samlede flow bor i **vidaxl-pris-lager** (hvor grupperings-kernen + combine-motoren + state allerede er) og
importerer `create_products_v2` + `product_utils` fra **dropxl** via `DROPXL_SCRIPTS`-env — præcis som
`combine_execute`-workflowen allerede gør (cross-repo checkout med `DROPXL_PAT`). Dropxl's `daily_create`/
`daily_create_large` **pensioneres** (schedule disables) ved cutover. Grupperings-modulerne er ÉN sandhedskilde.

---

## 7. "Eksisterende produkt + ny variant" — præcist hvad der sker

Eksempel: "Bogreol Hvid" findes (1 farve). vidaXL tilføjer SKU i "Sort".
1. Change-detektion: masteren har fået en ny SKU → processeres.
2. Regroup: den nye SKU's residual = "Bogreol" → samme gruppe som den eksisterende → gruppe = {Hvid, Sort}.
3. `old_products_for_skus({Hvid,Sort})` → finder det eksisterende "Bogreol Hvid"-produkt (anker).
4. `build_spec` på {Hvid,Sort}: Farve er nu 2 værdier → titel-motor fjerner farven → **"Bogreol"**, opretter
   Farve-akse (Hvid, Sort), 1. variant sku-only + øvrige metafelter.
5. `productSet(spec, product_id=anker)` in-place → produktet får ny titel + 2 farve-varianter, handle/SEO bevaret.

**Nøgle:** fordi grupperingen kører per RESIDUAL (ikke bare master), havner "Sort" på Bogreol-produktet og ikke
på et andet residual-produkt under samme master. Det er det create IKKE kan i dag.

Kant-tilfælde at teste eksplicit:
- Ny SKU hvis residual matcher et EKSISTERENDE residual-produkt → merge dertil (ikke opret nyt).
- Ny SKU hvis residual er NYT (ingen match) → opret som nyt produkt under masteren.
- Ny SKU der tipper en akse 1→2 (titel-regen) — verificér titlen skifter + gamle handle beholdes + 301 ikke nødvendig.
- Ny SKU der giver dublet-kombo → dedup + create_single (aldrig tab).

---

## 8. Byggefaser (intet live før fase 3 grøn)

**Fase 0 — fundament**
- Refaktorér `combine_exec` kerne-loop → `process_group(group, create_if_missing=True)` (delt af combine-backlog + nyt flow).
- Bekræft kolonne-orden-reglen (Farve→Størrelse→rest) er den eneste.

**Fase 1 — samlet motor (`unified_sync.py`)**
- Feed + `vidaxl_sku_master` → change-detektion (§3) → ændrede masters → regroup → grupper.
- `process_group` pr. gruppe (create-or-update). Kandidat-filtre for create. Daglig budget + resumbar. Snapshot-fil.

**Fase 2 — dry-run over HELE feedet**
- Kør uden at røre live: for hver gruppe, hvad ville ske (opret/merge/uændret) + fuld diff mod nuværende Shopify.
- Sammenlign mod hvad dropxl-create ville lave i dag → dokumentér alle afvigelser + verificér de er ønskede.

**Fase 3 — lille live-test**
- 5–10 ægte ændrede masters (nogle nye, nogle ny-variant-til-eksisterende, nogle dublet). Kør live. Validér
  slavisk (som combine: SKU-sæt, titel, 1. variant, metafelter, stocked, pris, handle, residual-match korrekt).

**Fase 4 — cutover**
- Disable dropxl `daily_create` + `daily_create_large`. Enable `unified_sync`-workflow (vidaxl-pris-lager,
  DROPXL_PAT + SHOPIFY_COMBINE_TOKEN + SUPABASE + FEED_URL, dagligt, resumbar, mail ved fejl).
- Behold `daily_delete`, `sync_inventory`, repricing uændret.

---

## 9. Risici & forholdsregler
- **Change-detektion misser en ny SKU** → produkt oprettes ikke. Modforanstaltning: periodisk fuld-scan-fallback
  (fx ugentligt: alle feed-SKU ikke i Shopify) som sikkerhedsnet.
- **Performance** ved fuld regroup: 16k masters — regroup er hurtigt; `old_products_for_skus` (1 query/SKU) er
  flaskehalsen → brug live-snapshot (som `recent_fix` allerede gør) i stedet for per-SKU-query.
- **Combine-backlog ikke færdig** ved cutover: lad den køre færdig først, ELLER lad `unified_sync` subsummere den
  (den gør jo det samme). Simplest: combine kører færdig, så tændes unified.
- **Titel-motor-afhængighed** (dropxl title_engine): allerede cross-repo-tilgængelig; verificér i dry-run.
- **Scopes/token**: SHOPIFY_COMBINE_TOKEN (write_products+write_content) dækker create+merge+redirects. Lager-
  aktivering sker under write_products (bevist). OK.

---

## 10. Definition af færdig
- Ét workflow der dagligt: opretter nye vidaXL-produkter + tilføjer nye varianter til eksisterende + regenererer
  titler ved akse-ændring — alt med SAMME gruppering som combine-motoren.
- Dry-run 0 uønskede afvigelser. Live-test slavisk grøn. Dropxl-create pensioneret. Ingen tabt SKU, ingen fejl-gruppering.
