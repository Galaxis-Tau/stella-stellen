#!/usr/bin/env python3
"""
STELLA NRW Stellenscraper – Version 4 (präzise)
Basiert auf der echten HTML-Struktur von STELLA NRW.

Tabellenklasse:  tableSuchAnzeige
Zeilenklassen:   lobw_ergebnis_odd / lobw_ergebnis_even
Paginierung:     ?block=500 → alle Treffer auf einer Seite
"""

import json
import re
import time
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ── Konfiguration ─────────────────────────────────────────────────────────────

BASE        = "https://www.schulministerium.nrw.de"
START_URL   = BASE + "/BiPo/Stella"
OUTPUT_FILE = "docs/stellen.json"

DORTMUND_ORT_VALUE = "913000"
RADIUS_KM          = "50"

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

# ── HTTP-Hilfsfunktionen ──────────────────────────────────────────────────────

def get(session, url):
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ GET-Fehler: {e}")
        return None

def post(session, url, data):
    try:
        r = session.post(url, data=data, headers=HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ POST-Fehler: {e}")
        return None

def abs_url(href):
    if not href:
        return ""
    return BASE + href if href.startswith("/") else href

# ── Ergebnisseite parsen ──────────────────────────────────────────────────────

def parse_ergebnisseite(soup, kategorie_id):
    """
    Liest alle Stellen aus einer STELLA-Ergebnisseite.
    Tabellenklasse: tableSuchAnzeige
    Zeilenklassen:  lobw_ergebnis_odd / lobw_ergebnis_even

    Spalten (0-basiert):
      0 = Stellenbezeichnung (mit <strong> für Titel)
      1 = Besoldungsgruppe
      2 = Dienstort / Dienststelle / Schule
      3 = Laufbahnrechtl. Voraussetzungen
      4 = Besondere Hinweise
      5 = Zeitpunkt der Besetzung
      6 = Dienststelle für Entgegennahme
      7 = Bewerbungsschluss (Datum)
    """
    stellen = []

    zeilen = soup.find_all(
        "tr",
        class_=re.compile(r"lobw_ergebnis_(odd|even)")
    )
    print(f"      {len(zeilen)} Tabellenzeilen gefunden")

    for zeile in zeilen:
        tds = zeile.find_all("td")
        if len(tds) < 3:
            continue

        def td(i):
            if i < len(tds):
                return tds[i].get_text(separator="\n", strip=True)
            return ""

        # Spalte 0: Titel (aus <strong>-Tag)
        strong = tds[0].find("strong")
        titel  = strong.get_text(strip=True) if strong else td(0).split("\n")[0]

        # Detaillink aus Spalte 4 ("Weitere Hinweise")
        link_tag   = tds[4].find("a", href=True) if len(tds) > 4 else None
        detail_url = abs_url(link_tag["href"]) if link_tag else ""

        # Spalte 2: Dienstort + Schule
        ort_raw = td(2)
        # Erste Zeile ist der Ort (Stadt), zweite ist öffentlich/Ersatz, dritte der Schulname
        ort_zeilen  = [z.strip() for z in ort_raw.split("\n") if z.strip()]
        ort         = ort_zeilen[0] if ort_zeilen else ""
        schulname   = ""
        for z in ort_zeilen[2:]:
            if len(z) > 5 and "öffentliche" not in z and "Ersatz" not in z:
                schulname = z
                break

        # Spalte 1: Besoldungsgruppe
        besoldung = td(1).split("\n")[0].strip()

        # Spalte 7: Bewerbungsschluss
        frist_raw = td(7)
        m = re.search(r"\d{2}\.\d{2}\.\d{4}", frist_raw)
        frist = m.group() if m else ""

        # Spalte 5: Besetzungszeitpunkt
        besetzung = td(5).split("\n")[0].strip()

        stelle = {
            "titel":      titel,
            "ort":        ort,
            "schulname":  schulname,
            "besoldung":  besoldung,
            "besetzung":  besetzung,
            "frist":      frist,
            "url":        detail_url,
            "kategorie":  kategorie_id,
            "abstand_km": None,
        }
        stellen.append(stelle)

    return stellen


def alle_auf_einmal_url(ergebnis_soup, aktuelle_url):
    """
    Sucht den Link für 'block=500' (alle Treffer auf einer Seite).
    Gibt die URL zurück oder None.
    """
    for a in ergebnis_soup.find_all("a", href=True):
        if "block=500" in a["href"]:
            return abs_url(a["href"])
    # Fallback: Parameter selbst einfügen
    if "suchid=" in aktuelle_url:
        url500 = re.sub(r"block=\d+", "block=500", aktuelle_url)
        if "block=" not in aktuelle_url:
            url500 = aktuelle_url + "&block=500"
        return url500
    return None


# ── Session + Navigation ──────────────────────────────────────────────────────

def setup_session():
    session = requests.Session()
    print("  Startseite laden…")
    soup = get(session, START_URL)
    if not soup:
        raise RuntimeError("Startseite nicht erreichbar")

    link = soup.find("a", href=re.compile(r"action="))
    if not link:
        raise RuntimeError("'Zu den Stellen'-Link nicht gefunden")

    auswahl_url = abs_url(link["href"])
    print(f"  Auswahlseite: {auswahl_url}")

    soup2 = get(session, auswahl_url)
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


# ── Formular abschicken ───────────────────────────────────────────────────────

def suche_mit_formular(session, form_url, kategorie_id, mit_radius):
    """
    Lädt das Suchformular, schickt es ab, wechselt dann auf block=500.
    """
    soup = get(session, form_url)
    if not soup:
        return []

    form = soup.find("form")
    if not form:
        # Evtl. direkte Listenansicht (keine Suchmaske)
        print(f"      Kein Formular — versuche direkte Listenansicht")
        return parse_und_alle(session, form_url, kategorie_id)

    action_url = abs_url(form.get("action", ""))

    # Alle hidden inputs
    post_data = {}
    for inp in form.find_all("input", type="hidden"):
        n = inp.get("name")
        v = inp.get("value", "")
        if n:
            post_data[n] = v

    post_data["button_suchen"] = "Suche starten"

    if mit_radius:
        ort_sel = soup.find("select", {"id": "ort"})
        umk_inp = soup.find("input", {"id": "umkreis"})
        if ort_sel:
            post_data[ort_sel["name"]] = DORTMUND_ORT_VALUE
            print(f"      Ort: {ort_sel['name']} = {DORTMUND_ORT_VALUE} (Dortmund)")
        if umk_inp:
            post_data[umk_inp["name"]] = RADIUS_KM
            print(f"      Umkreis: {umk_inp['name']} = {RADIUS_KM} km")

    print(f"      POST → {action_url}")
    ergebnis = post(session, action_url, post_data)
    if not ergebnis:
        return []

    return parse_und_alle(session, action_url, kategorie_id, ergebnis_soup=ergebnis)


def parse_und_alle(session, basis_url, kategorie_id, ergebnis_soup=None):
    """
    Wechselt auf block=500 damit alle Treffer auf einer Seite erscheinen,
    dann parst eine einzelne Seite.
    """
    if ergebnis_soup is None:
        ergebnis_soup = get(session, basis_url)
        if not ergebnis_soup:
            return []

    # Anzahl gefundener Stellen aus dem Text lesen
    treffer_text = ergebnis_soup.get_text()
    m = re.search(r"(\d+)\s+Stellenausschreibungen?\s+gefunden", treffer_text)
    if m:
        print(f"      STELLA meldet: {m.group(1)} Treffer")
    else:
        print(f"      (Trefferzahl nicht gefunden)")

    # block=500 URL finden und abrufen
    url500 = alle_auf_einmal_url(ergebnis_soup, basis_url)
    if url500:
        print(f"      Wechsel auf block=500: {url500}")
        time.sleep(1)
        ergebnis_soup = get(session, url500)
        if not ergebnis_soup:
            return []

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


# ── Schulbereich: 3 Unterseiten ───────────────────────────────────────────────

def scrape_schulbereich(session, url):
    print("\n📂 Schulstellen (Dortmund + 50 km)")
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


# ── Hauptprogramm ─────────────────────────────────────────────────────────────

def main():
    print(f"\n🔍 STELLA NRW Scraper v4 — {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    try:
        session, kat = setup_session()
    except RuntimeError as e:
        print(f"❌ {e}")
        return

    alle = []

    # Schulstellen
    if "schulbereich" in kat:
        st = scrape_schulbereich(session, kat["schulbereich"])
        alle.extend(st)
        print(f"→ Schulstellen gesamt: {len(st)}")
    time.sleep(2)

    # ZfsL
    if "zfsl" in kat:
        print("\n📂 ZfsL / Fachleiter (Dortmund + 50 km)")
        st = suche_mit_formular(session, kat["zfsl"], "zfsl", mit_radius=True)
        print(f"→ ZfsL gesamt: {len(st)}")
        alle.extend(st)
    time.sleep(2)

    # Schulaufsicht
    if "schulaufsicht" in kat:
        print("\n📂 Schulaufsicht (ganz NRW)")
        st = suche_mit_formular(session, kat["schulaufsicht"], "schulaufsicht", mit_radius=False)
        print(f"→ Schulaufsicht gesamt: {len(st)}")
        alle.extend(st)
    time.sleep(2)

    # Sonstige
    if "sonstige" in kat:
        print("\n📂 Sonstige Tätigkeiten (ganz NRW)")
        st = suche_mit_formular(session, kat["sonstige"], "sonstige", mit_radius=False)
        print(f"→ Sonstige gesamt: {len(st)}")
        alle.extend(st)

    # Ausgabe
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
        print(f"   {k}: {n}")


if __name__ == "__main__":
    main()
