#!/usr/bin/env python3
"""
STELLA NRW Stellenscraper – Version 6
- Vollständiger Zellentext Spalte 0 (ganzer <td>-Inhalt sauber)
- Geocoding auf Schulebene: Schulname + Ort → genaue Koordinaten
- Radius 25 km um Dortmund
- first_seen Tracking
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
SEEN_FILE   = "docs/first_seen.json"

DORTMUND_ORT_VALUE = "913000"
RADIUS_KM          = "25"

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

def geo_suche(query: str) -> tuple:
    """Sucht via Nominatim. Gibt (lat, lon) oder (None, None) zurück."""
    key = query.strip().lower()
    if key in _geo_cache:
        return _geo_cache[key]
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "json",
                "limit": 1,
                "countrycodes": "de",
                "addressdetails": 0,
            },
            headers={"User-Agent": "STELLA-NRW-Scraper/6.0 (schulstellen@nrw)"},
            timeout=12,
        )
        data = r.json()
        if data:
            result = (float(data[0]["lat"]), float(data[0]["lon"]))
            _geo_cache[key] = result
            time.sleep(1.1)  # Nominatim fair-use: max 1 req/s
            return result
    except Exception as e:
        print(f"    Geo-Fehler '{query}': {e}")
    _geo_cache[key] = (None, None)
    time.sleep(0.5)
    return None, None


def koordinaten_fuer_stelle(schulname: str, ort: str) -> tuple:
    """
    Versucht in dieser Reihenfolge:
    1. Schulname + Ort + NRW
    2. Schulname + NRW
    3. Nur Ort + NRW
    """
    if schulname and ort:
        # Schulname bereinigen (Kürzel wie "Städt." entfernen stört manchmal)
        lat, lon = geo_suche(f"{schulname}, {ort}, Nordrhein-Westfalen")
        if lat:
            return lat, lon
        # Fallback: nur erste Wörter des Schulnamens
        kurzname = " ".join(schulname.split()[:4])
        lat, lon = geo_suche(f"{kurzname}, {ort}, Nordrhein-Westfalen")
        if lat:
            return lat, lon

    if ort:
        lat, lon = geo_suche(f"{ort}, Nordrhein-Westfalen")
        if lat:
            return lat, lon

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

def zellentext_vollstaendig(td) -> str:
    """
    Extrahiert den vollständigen Text einer <td>-Zelle.
    Zeilenumbrüche entstehen bei <br>, <p>, Block-Elementen.
    Inline-Links (<a>) werden als Text behalten.
    Unsichtbare Elemente (class='unsichtbar') werden entfernt.
    """
    # Unsichtbare Elemente vorab entfernen (STELLA-Screenreader-Texte)
    for el in td.find_all(class_="unsichtbar"):
        el.decompose()

    # Zeilenumbrüche normalisieren
    for br in td.find_all("br"):
        br.replace_with("\n")
    for p in td.find_all("p"):
        p.insert_before("\n")
        p.unwrap()

    text = td.get_text(separator="\n")
    # Zeilen bereinigen
    zeilen = []
    for z in text.split("\n"):
        z = z.strip()
        if z:
            zeilen.append(z)
    return "\n".join(zeilen)


def parse_ergebnisseite(soup, kategorie_id):
    stellen = []
    zeilen = soup.find_all("tr", class_=re.compile(r"lobw_ergebnis_(odd|even)"))
    print(f"      {len(zeilen)} Zeilen")

    for zeile in zeilen:
        tds = zeile.find_all("td")
        if len(tds) < 3:
            continue

        # ── Spalte 0: Vollständiger Zelleninhalt ──────────────────────────────
        td0_voll = zellentext_vollstaendig(tds[0])
        zeilen0  = [z for z in td0_voll.split("\n") if z]

        # Titel = erste Zeile (war ursprünglich <strong>)
        strong   = tds[0].find("strong")
        titel    = strong.get_text(strip=True) if strong else (zeilen0[0] if zeilen0 else "")

        # Beschreibung = ALLE weiteren Zeilen aus Spalte 0
        if titel and zeilen0 and zeilen0[0] == titel:
            beschreibung_zeilen = zeilen0[1:]
        else:
            beschreibung_zeilen = zeilen0[1:] if zeilen0 else []

        # "Weitere Hinweise"-Zeile rausfiltern (ist Link-Text, kein Inhalt)
        beschreibung_zeilen = [z for z in beschreibung_zeilen
                               if "weitere hinweise" not in z.lower()]
        beschreibung = "\n".join(beschreibung_zeilen).strip()

        # ── Detail-Link (Spalte 4) ────────────────────────────────────────────
        link_tag   = tds[4].find("a", href=True) if len(tds) > 4 else None
        detail_url = abs_url(link_tag["href"]) if link_tag else ""

        # ── Spalte 2: Dienstort + Schulname ──────────────────────────────────
        td2_voll   = zellentext_vollstaendig(tds[2])
        ort_zeilen = [z for z in td2_voll.split("\n") if z]
        ort        = ort_zeilen[0] if ort_zeilen else ""
        schulname  = ""
        for z in ort_zeilen:
            if ("öffentliche" in z.lower() or "ersatzschule" in z.lower()
                    or z == ort):
                continue
            if len(z) > 5:
                schulname = z
                break

        # ── Spalte 1: Besoldung ───────────────────────────────────────────────
        td1_voll  = zellentext_vollstaendig(tds[1])
        besoldung = td1_voll.split("\n")[0].strip()

        # ── Spalte 7: Bewerbungsfrist ─────────────────────────────────────────
        td7 = zellentext_vollstaendig(tds[7]) if len(tds) > 7 else ""
        m   = re.search(r"\d{2}\.\d{2}\.\d{4}", td7)
        frist = m.group() if m else ""

        # ── Spalte 5: Besetzungszeitpunkt ─────────────────────────────────────
        td5       = zellentext_vollstaendig(tds[5]) if len(tds) > 5 else ""
        besetzung = td5.split("\n")[0].strip()

        stelle = {
            "titel":        titel[:200],
            "beschreibung": beschreibung,
            "ort":          ort,
            "schulname":    schulname,
            "besoldung":    besoldung,
            "besetzung":    besetzung,
            "frist":        frist,
            "url":          detail_url,
            "kategorie":    kategorie_id,
            "lat":          None,
            "lon":          None,
            "first_seen":   None,
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
        if "Schulbereich" in text:
            kat["schulbereich"] = url
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
    post_data  = {i["name"]: i.get("value", "")
                  for i in form.find_all("input", type="hidden") if i.get("name")}
    post_data["button_suchen"] = "Suche starten"

    if mit_radius:
        ort_sel = soup.find("select", {"id": "ort"})
        umk_inp = soup.find("input",  {"id": "umkreis"})
        if ort_sel:
            post_data[ort_sel["name"]] = DORTMUND_ORT_VALUE
        if umk_inp:
            post_data[umk_inp["name"]] = RADIUS_KM

    ergebnis = post(session, action_url, post_data)
    if not ergebnis:
        return []
    return parse_und_alle(session, action_url, kategorie_id, ergebnis)


def parse_und_alle(session, basis_url, kategorie_id, ergebnis_soup=None):
    if ergebnis_soup is None:
        ergebnis_soup = get(session, basis_url)
        if not ergebnis_soup:
            return []

    m = re.search(r"(\d+)\s+Stellenausschreibungen?\s+gefunden",
                  ergebnis_soup.get_text())
    if m:
        print(f"      STELLA: {m.group(1)} Treffer")

    url500 = alle_auf_einmal_url(ergebnis_soup)
    if url500:
        time.sleep(1)
        ergebnis_soup = get(session, url500)
        if not ergebnis_soup:
            return []

    stellen = parse_ergebnisseite(ergebnis_soup, kategorie_id)

    gesehen = set()
    unique  = []
    for s in stellen:
        key = s["url"] or (s["titel"] + s["ort"])
        if key and key not in gesehen:
            gesehen.add(key)
            unique.append(s)
    return unique


def scrape_schulbereich(session, url):
    print("\n📂 Schulstellen (Dortmund + 25 km)")
    soup = get(session, url)
    if not soup:
        return []
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


# ── Geocoding aller relevanten Stellen ───────────────────────────────────────

def geocodiere_alle(stellen, kategorien=("schulstellen", "zfsl")):
    """
    Geocodiert jede Stelle einzeln mit Schulname + Ort.
    Nutzt Cache um doppelte Anfragen zu vermeiden.
    """
    print("\n🗺  Geocoding (Schulen + ZfsL)…")
    relevant = [s for s in stellen if s["kategorie"] in kategorien]
    print(f"  {len(relevant)} Stellen zu geocodieren")

    for i, s in enumerate(relevant):
        lat, lon = koordinaten_fuer_stelle(s["schulname"], s["ort"])
        s["lat"] = lat
        s["lon"] = lon
        status = f"{lat:.4f}, {lon:.4f}" if lat else "nicht gefunden"
        name   = (s["schulname"] or s["ort"])[:50]
        print(f"  [{i+1}/{len(relevant)}] {name}: {status}")


# ── first_seen ────────────────────────────────────────────────────────────────

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
    return s["url"] or (s["titel"] + "|" + s["ort"])

def setze_first_seen(stellen, mapping):
    heute = date.today().isoformat()
    for s in stellen:
        key = stelle_key(s)
        if key not in mapping:
            mapping[key] = heute
        s["first_seen"] = mapping[key]


# ── Hauptprogramm ─────────────────────────────────────────────────────────────

def main():
    print(f"\n🔍 STELLA NRW Scraper v6 — {datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
        st = suche_mit_formular(session, kat["schulaufsicht"],
                                "schulaufsicht", mit_radius=False)
        print(f"→ {len(st)} Schulaufsicht")
        alle.extend(st)
    time.sleep(2)

    if "sonstige" in kat:
        print("\n📂 Sonstige Tätigkeiten (ganz NRW)")
        st = suche_mit_formular(session, kat["sonstige"],
                                "sonstige", mit_radius=False)
        print(f"→ {len(st)} Sonstige")
        alle.extend(st)

    geocodiere_alle(alle)
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
    for k in ["schulstellen", "zfsl", "schulaufsicht", "sonstige"]:
        n = sum(1 for s in alle if s["kategorie"] == k)
        geo = sum(1 for s in alle if s["kategorie"] == k and s["lat"])
        print(f"   {k}: {n} Stellen, {geo} geocodiert")


if __name__ == "__main__":
    main()
