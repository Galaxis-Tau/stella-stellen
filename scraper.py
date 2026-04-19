#!/usr/bin/env python3
"""
STELLA NRW Stellenscraper
Ruft Stellen von stella.nrw.de ab und speichert sie als stellen.json
"""

import json
import time
import re
from datetime import datetime, date
from math import radians, cos, sin, asin, sqrt
import requests
from bs4 import BeautifulSoup

# ── Konfiguration ────────────────────────────────────────────────────────────

BASE_URL = "https://www.stellenausschreibungen.nrw.de/suche"
STELLA_BASE = "https://www.stellenausschreibungen.nrw.de"

# Koordinaten Dortmund
DORTMUND_LAT = 51.5136
DORTMUND_LON = 7.4653
RADIUS_KM = 50

# Kategorien die OHNE Kilometerbegrenzung angezeigt werden
KATEGORIEN_OHNE_RADIUS = {
    "schulaufsicht",
    "sonstige",
}

OUTPUT_FILE = "docs/stellen.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9",
}

# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    """Luftlinienabstand in km zwischen zwei GPS-Punkten."""
    r = 6371
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def koordinaten_von_ort(ort: str) -> tuple[float, float] | None:
    """Gibt (lat, lon) für einen Ortsnamen via Nominatim zurück."""
    if not ort or ort.strip() == "":
        return None
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": f"{ort}, Nordrhein-Westfalen, Deutschland",
            "format": "json",
            "limit": 1,
            "countrycodes": "de",
        }
        r = requests.get(url, params=params, headers={"User-Agent": "STELLA-Scraper/1.0"}, timeout=10)
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None


def abstand_do(lat, lon):
    """Abstand von Dortmund in km."""
    return haversine(DORTMUND_LAT, DORTMUND_LON, lat, lon)


# ── Scraping ─────────────────────────────────────────────────────────────────

def lade_seite(session: requests.Session, url: str, params: dict = None) -> BeautifulSoup | None:
    try:
        r = session.get(url, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ Fehler beim Laden von {url}: {e}")
        return None


def parse_stellen_liste(soup: BeautifulSoup) -> list[dict]:
    """Extrahiert Stellenkarten aus einer Suchergebnisseite."""
    stellen = []
    # STELLA verwendet üblicherweise Tabellenzeilen oder Listenelemente
    # Wir versuchen mehrere Selektoren
    rows = (
        soup.select("table.result-table tbody tr")
        or soup.select(".stellenausschreibung")
        or soup.select(".search-result-item")
        or soup.select("tr.stelle")
        or soup.select(".ergebnis-zeile")
    )

    for row in rows:
        try:
            stelle = parse_zeile(row)
            if stelle:
                stellen.append(stelle)
        except Exception as e:
            print(f"  ⚠ Parse-Fehler: {e}")
    return stellen


def parse_zeile(row) -> dict | None:
    """Wandelt eine HTML-Zeile in ein Stellen-Dict um."""
    text = row.get_text(separator=" ", strip=True)
    if len(text) < 10:
        return None

    # Link zur Detailseite
    link_tag = row.find("a", href=True)
    detail_url = ""
    if link_tag:
        href = link_tag["href"]
        detail_url = href if href.startswith("http") else STELLA_BASE + href

    # Titel
    titel_tag = (
        row.find("td", class_=re.compile(r"titel|stelle|bezeichnung", re.I))
        or row.find(["h3", "h4", "strong"])
        or link_tag
    )
    titel = titel_tag.get_text(strip=True) if titel_tag else text[:80]

    # Ort
    ort_tag = row.find("td", class_=re.compile(r"ort|schule|dienststelle|standort", re.I))
    ort = ort_tag.get_text(strip=True) if ort_tag else ""

    # Bewerbungsfrist
    frist_tag = row.find("td", class_=re.compile(r"frist|datum|bewerbung", re.I))
    frist = frist_tag.get_text(strip=True) if frist_tag else ""

    # Stellenart / Schulform
    art_tag = row.find("td", class_=re.compile(r"art|typ|schulform|kategorie", re.I))
    art = art_tag.get_text(strip=True) if art_tag else ""

    # Alle Zellentexte als Fallback
    zellen = [td.get_text(strip=True) for td in row.find_all("td")]

    return {
        "titel": titel or (zellen[0] if zellen else ""),
        "ort": ort or (zellen[1] if len(zellen) > 1 else ""),
        "frist": frist or (zellen[-1] if zellen else ""),
        "art": art,
        "url": detail_url,
        "rohdaten": " | ".join(zellen[:5]),
    }


def scrape_kategorie(session: requests.Session, suchbegriffe: list[str], kategorie_id: str) -> list[dict]:
    """
    Führt Suchanfragen für gegebene Begriffe durch und gibt alle Treffer zurück.
    STELLA hat kein echtes API – wir nutzen die Suchmaske.
    """
    ergebnisse = []
    gesehen = set()

    for begriff in suchbegriffe:
        print(f"  Suche nach: '{begriff}'")
        params = {
            "search": begriff,
            "submit": "Suchen",
        }
        soup = lade_seite(session, BASE_URL, params)
        if not soup:
            continue

        stellen = parse_stellen_liste(soup)

        # Paginierung
        seite = 1
        while True:
            next_link = soup.find("a", string=re.compile(r"weiter|nächste|next|›|»", re.I))
            if not next_link or seite > 10:
                break
            seite += 1
            next_url = next_link["href"]
            if not next_url.startswith("http"):
                next_url = STELLA_BASE + next_url
            soup = lade_seite(session, next_url)
            if not soup:
                break
            stellen.extend(parse_stellen_liste(soup))
            time.sleep(1)

        for s in stellen:
            key = s["url"] or s["titel"]
            if key not in gesehen:
                gesehen.add(key)
                s["kategorie"] = kategorie_id
                ergebnisse.append(s)

        time.sleep(1.5)

    return ergebnisse


def filtern_nach_radius(stellen: list[dict], kategorie_id: str) -> list[dict]:
    """Filtert Stellen auf 50 km um Dortmund, sofern die Kategorie das erfordert."""
    if kategorie_id in KATEGORIEN_OHNE_RADIUS:
        for s in stellen:
            s["abstand_km"] = None
        return stellen

    gefiltert = []
    for s in stellen:
        ort = s.get("ort", "")
        if not ort:
            s["abstand_km"] = None
            gefiltert.append(s)  # Bei unbekanntem Ort mit aufnehmen
            continue

        coords = koordinaten_von_ort(ort)
        if coords:
            lat, lon = coords
            km = round(abstand_do(lat, lon), 1)
            s["abstand_km"] = km
            s["lat"] = lat
            s["lon"] = lon
            if km <= RADIUS_KM:
                gefiltert.append(s)
        else:
            s["abstand_km"] = None
            gefiltert.append(s)  # Unbekannte Orte aufnehmen
        time.sleep(0.5)  # Nominatim fair use

    return gefiltert


# ── Hauptprogramm ─────────────────────────────────────────────────────────────

KATEGORIEN = [
    {
        "id": "schulstellen",
        "label": "Schulstellen",
        "radius": True,
        "suchbegriffe": [
            "Schulleitung",
            "stellvertretende Schulleitung",
            "Abteilungsleitung Schule",
            "Koordination Schule",
            "Beförderungsstelle Schule",
            "Konrektorin",
            "Rektorin",
            "Oberstudienrat",
            "Studiendirektor",
        ],
    },
    {
        "id": "zfsl",
        "label": "ZfsL / Fachleiter",
        "radius": True,
        "suchbegriffe": [
            "Fachleiter",
            "Fachleiterin",
            "Zentrum für schulpraktische Lehrerausbildung",
            "ZfsL",
            "Seminarausbildung",
            "Ausbildungsbeauftragter",
        ],
    },
    {
        "id": "schulaufsicht",
        "label": "Schulaufsicht",
        "radius": False,
        "suchbegriffe": [
            "Schulaufsicht",
            "Schulrätin",
            "Schulrat",
            "Leiterin Schulamt",
            "Bezirksregierung Schule",
            "obere Schulaufsicht",
            "untere Schulaufsicht",
            "Dezernentin Schule",
            "Dezernat Schule",
        ],
    },
    {
        "id": "sonstige",
        "label": "Sonstige Tätigkeiten",
        "radius": False,
        "suchbegriffe": [
            "Abordnung Schule",
            "Teilabordnung",
            "pädagogische Mitarbeit",
            "Qualitätsanalyse",
            "QA-Prüferin",
            "Lehrerausbildung Ministerium",
            "Medienberatung",
            "Schulpsychologie",
            "sonstige Tätigkeit Schule",
        ],
    },
]


def main():
    print(f"\n🔍 STELLA NRW Scraper — {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    alle_stellen = []

    for kat in KATEGORIEN:
        print(f"\n📂 Kategorie: {kat['label']}")
        stellen = scrape_kategorie(session, kat["suchbegriffe"], kat["id"])
        print(f"  → {len(stellen)} Treffer gefunden")

        if kat["radius"]:
            print(f"  → Filterung auf {RADIUS_KM} km um Dortmund...")
            stellen = filtern_nach_radius(stellen, kat["id"])
            print(f"  → {len(stellen)} Stellen im Radius")
        else:
            for s in stellen:
                s["abstand_km"] = None
            print(f"  → Kein Radius-Filter (ganz NRW)")

        alle_stellen.extend(stellen)

    # Ausgabe
    output = {
        "aktualisiert": datetime.now().isoformat(),
        "aktualisiert_lesbar": datetime.now().strftime("%d.%m.%Y um %H:%M Uhr"),
        "gesamt": len(alle_stellen),
        "stellen": alle_stellen,
    }

    import os
    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Fertig! {len(alle_stellen)} Stellen gespeichert in '{OUTPUT_FILE}'")
    print(f"   Zeitstempel: {output['aktualisiert_lesbar']}")


if __name__ == "__main__":
    main()
