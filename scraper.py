#!/usr/bin/env python3
"""
STELLA NRW Stellenscraper – Version 5
- Radius 25 km um Dortmund
- Vollständiger Zellentext Spalte 0 (nicht nur <strong>)
- Geocoding der Ortsnamen via Nominatim → lat/lon in JSON
- first_seen: Datum der Erstentdeckung wird dauerhaft gespeichert
"""

import json
import re
import time
import os
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup

# ── Konfiguration ─────────────────────────────────────────────────────────────

BASE        = "https://www.schulministerium.nrw.de"
START_URL   = BASE + "/BiPo/Stella"
OUTPUT_FILE = "docs/stellen.json"
SEEN_FILE   = "docs/first_seen.json"   # persistente Erstentdeckungs-Datei

DORTMUND_ORT_VALUE = "913000"
RADIUS_KM          = "25"             # ← auf 25 km reduziert

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE,
}

# ── Geocoding ─────────────────────────────────────────────────────────────────

_geo_cache = {}

# Bekannte NRW-Städte mit festen Koordinaten → spart Nominatim-Anfragen
GEO_VORRAT = {
    "dortmund":       (51.5136, 7.4653),
    "bochum":         (51.4818, 7.2162),
    "essen":          (51.4556, 7.0116),
    "herne":          (51.5369, 7.2197),
    "gelsenkirchen":  (51.5177, 7.0857),
    "recklinghausen": (51.6141, 7.1974),
    "hamm":           (51.6739, 7.8152),
    "datteln":        (51.6544, 7.3411),
    "castrop-rauxel": (51.5528, 7.3127),
    "lünen":          (51.6161, 7.5255),
    "werne":          (51.6647, 7.6336),
    "selm":           (51.6989, 7.4697),
    "bergkamen":      (51.6161, 7.6297),
    "kamen":          (51.5936, 7.6639),
    "unna":           (51.5353, 7.6886),
    "schwerte":       (51.4461, 7.5636),
    "iserlohn":       (51.3748, 7.6949),
    "hagen":          (51.3671, 7.4633),
    "witten":         (51.4439, 7.3350),
    "herdecke":       (51.4014, 7.4317),
    "wetter":         (51.3883, 7.3953),
    "marl":           (51.6572, 7.0883),
    "haltern am see": (51.7406, 7.1817),
    "herten":         (51.5961, 7.1358),
    "gladbeck":       (51.5706, 6.9897),
    "bottrop":        (51.5236, 6.9289),
    "oberhausen":     (51.4697, 6.8517),
    "dinslaken":      (51.5661, 6.7394),
    "duisburg":       (51.4344, 6.7623),
    "mülheim an der ruhr": (51.4275, 6.8825),
    "düsseldorf":     (51.2217, 6.7762),
    "wuppertal":      (51.2562, 7.1508),
    "solingen":       (51.1731, 7.0831),
    "remscheid":      (51.1786, 7.1894),
    "münster":        (51.9607, 7.6261),
    "köln":           (50.9333, 6.9500),
    "bonn":           (50.7358, 7.0982),
    "siegen":         (50.8748, 8.0243),
    "arnsberg":       (51.3956, 8.0636),
    "lüdenscheid":    (51.2197, 7.6286),
    "plettenberg":    (51.2072, 7.8697),
    "iserlohn":       (51.3748, 7.6949),
}

def koordinaten(ort: str):
    if not ort:
        return None, None
    key = ort.strip().lower()
    if key in _geo_cache:
        return _geo_cache[key]
    # Vorrat prüfen
    if key in GEO_VORRAT:
        _geo_cache[key] = GEO_VORRAT[key]
        return GEO_VORRAT[key]
    # Nominatim
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{ort}, Nordrhein-Westfalen", "format": "json",
                    "limit": 1, "countrycodes": "de"},
            headers={"User-Agent": "STELLA-Scraper/5.0"},
            timeout=10
        )
        data = r.json()
        if data:
            result = (float(data[0]["lat"]), float(data[0]["lon"]))
            _geo_cache[key] = result
            time.sleep(1.1)   # Nominatim fair-use: max 1 req/s
            return result
    except Exception as e:
        print(f"    Geo-Fehler für '{ort}': {e}")
    _geo_cache[key] = (None, None)
    return None, None

# ── HTTP ──────────────────────────────────────────────────────────────────────

def get(session, url):
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ GET: {e}")
        return None

def post(session, url, data):
    try:
        r = session.post(url, data=data, headers=HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ POST: {e}")
        return None

def abs_url(href):
    if not href:
        return ""
    return BASE + href if href.startswith("/") else href

# ── Ergebnisse parsen ─────────────────────────────────────────────────────────

def parse_ergebnisseite(soup, kategorie_id):
    stellen = []
    zeilen = soup.find_all("tr", class_=re.compile(r"lobw_ergebnis_(odd|even)"))
    print(f"      {len(zeilen)} Zeilen")

    for zeile in zeilen:
        tds = zeile.find_all("td")
        if len(tds) < 3:
            continue

        def td_text(i):
            if i < len(tds):
                return tds[i].get_text(separator="\n", strip=True)
            return ""

        # ── Spalte 0: Vollständiger Text (nicht nur <strong>) ──
        # Titel = <strong>-Text
        strong = tds[0].find("strong")
        titel = strong.get_text(strip=True) if strong else ""

        # Beschreibung = gesamter Zellentext OHNE den <strong>-Part
        # Wir holen alle Textknoten die nach dem <strong> kommen
        zelle0_voll = tds[0].get_text(separator="\n", strip=True)
        # Titel-Zeile aus dem Volltext entfernen
        beschreibung = ""
        if titel and titel in zelle0_voll:
            rest = zelle0_voll.replace(titel, "", 1).strip()
            # Erste nicht-leere Zeilen als Beschreibung
            zeilen_rest = [z.strip() for z in rest.split("\n") if z.strip()]
            beschreibung = "\n".join(zeilen_rest[:4])  # max 4 Zeilen
        elif not titel:
            zeilen0 = [z.strip() for z in zelle0_voll.split("\n") if z.strip()]
            titel = zeilen0[0] if zeilen0 else ""
            beschreibung = "\n".join(zeilen0[1:4]) if len(zeilen0) > 1 else ""

        # Detail-Link (aus Spalte 4 "Weitere Hinweise")
        link_tag   = tds[4].find("a", href=True) if len(tds) > 4 else None
        detail_url = abs_url(link_tag["href"]) if link_tag else ""

        # ── Spalte 2: Dienstort + Schulname ──
        ort_raw    = td_text(2)
        ort_zeilen = [z.strip() for z in ort_raw.split("\n") if z.strip()]
        ort        = ort_zeilen[0] if ort_zeilen else ""
        schulname  = ""
        for z in ort_zeilen[2:]:
            if len(z) > 5 and "öffentliche" not in z and "Ersatz" not in z:
                schulname = z
                break

        # ── Spalte 1: Besoldung ──
        besoldung = td_text(1).split("\n")[0].strip()

        # ── Spalte 7: Bewerbungsfrist ──
        frist_raw = td_text(7)
        m = re.search(r"\d{2}\.\d{2}\.\d{4}", frist_raw)
        frist = m.group() if m else ""

        # ── Spalte 5: Besetzungszeitpunkt ──
        besetzung = td_text(5).split("\n")[0].strip()

        stelle = {
            "titel":       titel[:200],
            "beschreibung": beschreibung,
            "ort":         ort,
            "schulname":   schulname,
            "besoldung":   besoldung,
            "besetzung":   besetzung,
            "frist":       frist,
            "url":         detail_url,
            "kategorie":   kategorie_id,
            "lat":         None,
            "lon":         None,
            "abstand_km":  None,
            "first_seen":  None,
        }
        stellen.append(stelle)

    return stellen

def alle_auf_einmal_url(soup):
    for a in soup.find_all("a", href=True):
        if "block=500" in a["href"]:
            return abs_url(a["href"])
    return None

# ── Session + Navigation ──────────────────────────────────────────────────────

def setup_session():
    session = requests.Session()
    print("  Startseite…")
    soup = get(session, START_URL)
    if not soup:
        raise RuntimeError("Startseite nicht erreichbar")
    link = soup.find("a", href=re.compile(r"action="))
    if not link:
        raise RuntimeError("Kein Link gefunden")
    soup2 = get(session, abs_url(link["href"]))
    if not soup2:
        raise RuntimeError("Auswahlseite nicht erreichbar")
    kat = {}
    for a in soup2.select("ul.suchAuswahl a"):
        href = a["href"]
        text = a.get_text(strip=True)
        url  = abs_url(href)
        if "Schulbereich" in text:      kat["schulbereich"] = url
        elif "Zentren" in text or "Fachleiter" in text or "stellenart=4" in href:
            kat["zfsl"] = url
        elif "Schulaufsicht" in text or "stellenart=2" in href:
            kat["schulaufsicht"] = url
        elif "Sonstige" in text or "stellenart=3" in href:
            kat["sonstige"] = url
    print(f"  Kategorien: {list(kat.keys())}")
    return session, kat

# ── Formular ──────────────────────────────────────────────────────────────────

def suche_mit_formular(session, form_url, kategorie_id, mit_radius):
    soup = get(session, form_url)
    if not soup:
        return []
    form = soup.find("form")
    if not form:
        return parse_und_alle(session, form_url, kategorie_id, soup)

    action_url = abs_url(form.get("action", ""))
    post_data  = {i["name"]: i.get("value","")
                  for i in form.find_all("input", type="hidden") if i.get("name")}
    post_data["button_suchen"] = "Suche starten"

    if mit_radius:
        ort_sel = soup.find("select", {"id": "ort"})
        umk_inp = soup.find("input",  {"id": "umkreis"})
        if ort_sel: post_data[ort_sel["name"]] = DORTMUND_ORT_VALUE
        if umk_inp: post_data[umk_inp["name"]] = RADIUS_KM

    ergebnis = post(session, action_url, post_data)
    if not ergebnis:
        return []
    return parse_und_alle(session, action_url, kategorie_id, ergebnis)

def parse_und_alle(session, basis_url, kategorie_id, ergebnis_soup=None):
    if ergebnis_soup is None:
        ergebnis_soup = get(session, basis_url)
        if not ergebnis_soup: return []

    m = re.search(r"(\d+)\s+Stellenausschreibungen?\s+gefunden",
                  ergebnis_soup.get_text())
    if m: print(f"      STELLA: {m.group(1)} Treffer")

    url500 = alle_auf_einmal_url(ergebnis_soup)
    if url500:
        time.sleep(1)
        ergebnis_soup = get(session, url500)
        if not ergebnis_soup: return []

    stellen = parse_ergebnisseite(ergebnis_soup, kategorie_id)

    # Deduplizieren
    gesehen = set()
    unique  = []
    for s in stellen:
        key = s["url"] or (s["titel"] + s["ort"])
        if key and key not in gesehen:
            gesehen.add(key)
            unique.append(s)
    return unique

# ── Schulbereich: Unterseiten ─────────────────────────────────────────────────

def scrape_schulbereich(session, url):
    print("\n📂 Schulstellen (Dortmund + 25 km)")
    soup = get(session, url)
    if not soup: return []
    unterseiten = [(a.get_text(strip=True), abs_url(a["href"]))
                   for a in soup.select("ul.suchAuswahl a")]
    if not unterseiten:
        unterseiten = [("Schulbereich", url)]
    alle = []
    for label, sub_url in unterseiten:
        print(f"  → {label}")
        st = suche_mit_formular(session, sub_url, "schulstellen", mit_radius=True)
        print(f"     {len(st)} Stellen")
        alle.extend(st)
        time.sleep(2)
    return alle

# ── Geocoding aller Stellen ───────────────────────────────────────────────────

def geocodiere(stellen, nur_kategorien=("schulstellen","zfsl")):
    print("\n🗺  Geocoding…")
    orte_eindeutig = set(s["ort"] for s in stellen
                         if s["kategorie"] in nur_kategorien and s["ort"])
    print(f"  {len(orte_eindeutig)} eindeutige Orte")

    for ort in sorted(orte_eindeutig):
        lat, lon = koordinaten(ort)
        if lat:
            print(f"  ✓ {ort}: {lat:.4f}, {lon:.4f}")
        else:
            print(f"  ? {ort}: nicht gefunden")

    for s in stellen:
        if s["kategorie"] in nur_kategorien and s["ort"]:
            lat, lon = koordinaten(s["ort"])
            s["lat"] = lat
            s["lon"] = lon

# ── first_seen tracken ────────────────────────────────────────────────────────

def lade_first_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def speichere_first_seen(mapping):
    os.makedirs("docs", exist_ok=True)
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

def stelle_key(s):
    """Stabiler Key für eine Stelle."""
    return s["url"] or (s["titel"] + "|" + s["ort"])

def setze_first_seen(stellen, first_seen_map):
    heute = date.today().isoformat()
    for s in stellen:
        key = stelle_key(s)
        if key not in first_seen_map:
            first_seen_map[key] = heute
        s["first_seen"] = first_seen_map[key]

# ── Hauptprogramm ─────────────────────────────────────────────────────────────

def main():
    print(f"\n🔍 STELLA NRW Scraper v5 — {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    first_seen_map = lade_first_seen()

    try:
        session, kat = setup_session()
    except RuntimeError as e:
        print(f"❌ {e}")
        return

    alle = []

    if "schulbereich" in kat:
        st = scrape_schulbereich(session, kat["schulbereich"])
        alle.extend(st)
    time.sleep(2)

    if "zfsl" in kat:
        print("\n📂 ZfsL / Fachleiter (Dortmund + 25 km)")
        st = suche_mit_formular(session, kat["zfsl"], "zfsl", mit_radius=True)
        print(f"→ {len(st)} ZfsL-Stellen")
        alle.extend(st)
    time.sleep(2)

    if "schulaufsicht" in kat:
        print("\n📂 Schulaufsicht (ganz NRW)")
        st = suche_mit_formular(session, kat["schulaufsicht"], "schulaufsicht", mit_radius=False)
        print(f"→ {len(st)} Schulaufsicht")
        alle.extend(st)
    time.sleep(2)

    if "sonstige" in kat:
        print("\n📂 Sonstige Tätigkeiten (ganz NRW)")
        st = suche_mit_formular(session, kat["sonstige"], "sonstige", mit_radius=False)
        print(f"→ {len(st)} Sonstige")
        alle.extend(st)

    # Geocoding (nur Schul + ZfsL)
    geocodiere(alle, nur_kategorien=("schulstellen", "zfsl"))

    # first_seen setzen
    setze_first_seen(alle, first_seen_map)
    speichere_first_seen(first_seen_map)

    os.makedirs("docs", exist_ok=True)
    output = {
        "aktualisiert":        datetime.now().isoformat(),
        "aktualisiert_lesbar": datetime.now().strftime("%d.%m.%Y um %H:%M Uhr"),
        "gesamt":              len(alle),
        "stellen":             alle,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {len(alle)} Stellen gespeichert.")
    for k in ["schulstellen","zfsl","schulaufsicht","sonstige"]:
        n = sum(1 for s in alle if s["kategorie"] == k)
        print(f"   {k}: {n}")

if __name__ == "__main__":
    main()
