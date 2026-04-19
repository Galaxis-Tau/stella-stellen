#!/usr/bin/env python3
"""
STELLA NRW Stellenscraper – Version 3 (final)
Basiert auf der echten HTML-Struktur. Nutzt das POST-Suchformular
direkt mit Orts- und Umkreisfilter von STELLA.

Ablauf:
  1. Startseite laden → Session-Cookie + Link zur Auswahlseite
  2. Auswahlseite laden → Links zu den 4 Kategorien
  3. Je Kategorie: Suchformular laden → POST mit Parametern abschicken
  4. Ergebnistabelle parsen + paginieren
  5. stellen.json schreiben
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

# Dortmund-Wert aus dem <select id="ort"> Dropdown
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

def soup_get(session, url):
    try:
        r = session.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ GET-Fehler {url}: {e}")
        return None

def soup_post(session, url, data):
    try:
        r = session.post(url, data=data, headers=HEADERS, timeout=25)
        r.raise_for_status()
        r.encoding = "iso-8859-1"
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ⚠ POST-Fehler {url}: {e}")
        return None

def abs_url(href):
    if not href:
        return ""
    return BASE + href if href.startswith("/") else href

# ── Schritt 1 + 2: Session aufbauen, Kategorielinks finden ───────────────────

def setup_session():
    """
    Startet eine Session, lädt die Startseite (Cookie!),
    folgt dem 'zu den Stellen'-Link → Auswahlseite,
    gibt die 4 Kategorielinks zurück.
    """
    session = requests.Session()

    # Startseite → Session-Cookie setzen
    print("  [1/4] Startseite laden…")
    soup = soup_get(session, START_URL)
    if not soup:
        raise RuntimeError("Startseite nicht erreichbar")

    # Link "zu den Stellen" finden
    link = soup.find("a", href=re.compile(r"action="))
    if not link:
        raise RuntimeError("Kein 'zu den Stellen'-Link gefunden")
    auswahl_url = abs_url(link["href"])
    print(f"  [2/4] Auswahlseite: {auswahl_url}")

    soup2 = soup_get(session, auswahl_url)
    if not soup2:
        raise RuntimeError("Auswahlseite nicht erreichbar")

    # Kategorielinks aus <ul class="suchAuswahl">
    kat_links = {}
    for a in soup2.select("ul.suchAuswahl a"):
        href = a["href"]
        text = a.get_text(strip=True)
        url  = abs_url(href)

        if "Schulbereich" in text:
            kat_links["schulbereich_url"] = url
        elif "Zentren" in text or "Fachleiter" in text or "stellenart=4" in href:
            kat_links["zfsl_url"] = url
        elif "Schulaufsicht" in text or "stellenart=2" in href:
            kat_links["schulaufsicht_url"] = url
        elif "Sonstige" in text or "stellenart=3" in href:
            kat_links["sonstige_url"] = url

    print(f"  [3/4] {len(kat_links)} Kategorielinks gefunden: {list(kat_links.keys())}")
    return session, kat_links

# ── Schritt 3: Suchformular laden und abschicken ──────────────────────────────

def lade_suchformular(session, url):
    """Lädt die Suchseite und extrahiert Formular-URL + versteckte Felder."""
    soup = soup_get(session, url)
    if not soup:
        return None, {}, None

    form = soup.find("form")
    if not form:
        return None, {}, soup

    action = abs_url(form.get("action", ""))

    # Alle hidden inputs
    hidden = {}
    for inp in form.find_all("input", type="hidden"):
        name  = inp.get("name")
        value = inp.get("value", "")
        if name:
            hidden[name] = value

    return action, hidden, soup


def finde_ort_umkreis_felder(soup):
    """
    Liest die Feldnamen für Ort und Umkreis aus dem Formular.
    Die Namen sind obfuskiert (z.B. param219d57f2c46dac8).
    """
    ort_name     = None
    umkreis_name = None

    ort_select = soup.find("select", {"id": "ort"})
    if ort_select:
        ort_name = ort_select.get("name")

    umkreis_input = soup.find("input", {"id": "umkreis"})
    if umkreis_input:
        umkreis_name = umkreis_input.get("name")

    return ort_name, umkreis_name


def finde_stellenart_feld(soup):
    """Liest den Feldnamen und Wert des stellenart hidden-inputs."""
    inp = soup.find("input", {"name": "stellenart"})
    if inp:
        return "stellenart", inp.get("value", "")
    return None, None


# ── Schritt 4: Ergebnisseite parsen ──────────────────────────────────────────

def parse_ergebnisse(soup, kategorie_id):
    """
    Parst eine STELLA-Ergebnisseite.
    STELLA zeigt Ergebnisse in <table>-Zeilen.
    Gibt (stellen_liste, naechste_url) zurück.
    """
    stellen = []

    # Alle Tabellen durchsuchen
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            texte = [td.get_text(separator=" ", strip=True) for td in tds]
            full  = " ".join(texte)

            # Navigation / Kopfzeilen überspringen
            if len(full) < 20:
                continue
            if any(x in full for x in [
                "Suchmaschine", "Suche starten", "zurück", "Seite",
                "Stellenbezeichnung", "Schulform", "Rechtsstatus",
            ]):
                continue

            # Detaillink
            link_tag  = tr.find("a", href=True)
            detail_url = abs_url(link_tag["href"]) if link_tag else ""

            # Titel: bevorzugt Linktext, sonst erste Zelle
            titel = link_tag.get_text(strip=True) if link_tag else texte[0]

            # Ort: zweite Spalte wenn sinnvoll, sonst heuristisch
            ort = ""
            for t in texte[1:4]:
                if (3 < len(t) < 60
                        and not re.search(r"\d{5}", t)
                        and not any(w in t.lower() for w in [
                            "rektor", "direktor", "leiter", "koordinat",
                            "aufsicht", "schulform", "gymnasium", "grundschule",
                            "berufskolleg", "gesamtschule", "realschule",
                            "hauptschule", "förderschule", "weiterbildung",
                            "zfsl", "seminar",
                        ])):
                    ort = t
                    break

            # Bewerbungsfrist: Datumsformat DD.MM.YYYY
            frist = ""
            for t in texte:
                m = re.search(r"\d{2}\.\d{2}\.\d{4}", t)
                if m:
                    frist = m.group()
                    break

            # Schulform / Art
            art = ""
            for sf in [
                "Gymnasium", "Gesamtschule", "Grundschule", "Realschule",
                "Hauptschule", "Berufskolleg", "Förderschule",
                "Weiterbildungskolleg", "Freie Waldorfschule",
                "Gemeinschaftsschule", "Sekundarschule",
                "ZfsL", "Schulamt", "Bezirksregierung",
                "QUA-LiS", "LAQUILA",
            ]:
                if sf.lower() in full.lower():
                    art = sf
                    break

            stelle = {
                "titel":     titel[:200],
                "ort":       ort,
                "frist":     frist,
                "art":       art,
                "url":       detail_url,
                "kategorie": kategorie_id,
                "rohdaten":  " | ".join(texte[:6])[:300],
                "abstand_km": None,
            }
            stellen.append(stelle)

    # Nächste Seite suchen
    naechste = None
    for a in soup.find_all("a", href=True):
        t = a.get_text(strip=True)
        if re.search(r"weiter|nächste|next|›|»|\d+\s*$", t, re.I):
            href = a["href"]
            if "action=" in href:
                naechste = abs_url(href)
                break

    return stellen, naechste


def deduplizieren(stellen):
    gesehen = set()
    result  = []
    for s in stellen:
        key = s["url"] or s["titel"]
        if key and key not in gesehen and len(key) > 4:
            gesehen.add(key)
            result.append(s)
    return result


# ── Kategorie-Scraper ─────────────────────────────────────────────────────────

def scrape_schulbereich(session, basis_url):
    """
    Schulstellen haben 3 Unterseiten:
      1_1 = Leitungsstellen
      1_2 = Weitere Funktionsstellen
      1_3 = Stellen/Beförderungsstellen
    Alle mit Dortmund + 50km filtern.
    """
    print("\n  📂 Schulstellen (mit Radius Dortmund 50km)")
    alle = []

    # Unterseiten-Links aus der Zwischenebene lesen
    soup = soup_get(session, basis_url)
    if not soup:
        return []

    unterseiten = []
    for a in soup.select("ul.suchAuswahl a"):
        unterseiten.append((a.get_text(strip=True), abs_url(a["href"])))

    if not unterseiten:
        unterseiten = [("Schulbereich", basis_url)]

    for label, url in unterseiten:
        print(f"    → Unterseite: {label}")
        stellen = scrape_mit_formular(
            session, url,
            kategorie_id="schulstellen",
            mit_radius=True,
        )
        alle.extend(stellen)
        time.sleep(2)

    return alle


def scrape_mit_formular(session, form_url, kategorie_id, mit_radius=False):
    """
    Lädt das Suchformular, schickt es mit optionalem Radius ab,
    paginiert und gibt alle Stellen zurück.
    """
    action_url, hidden_data, soup = lade_suchformular(session, form_url)
    if not soup or not action_url:
        return []

    # POST-Daten zusammenbauen
    post_data = dict(hidden_data)
    post_data["button_suchen"] = "Suche starten"

    if mit_radius:
        ort_name, umkreis_name = finde_ort_umkreis_felder(soup)
        if ort_name:
            post_data[ort_name] = DORTMUND_ORT_VALUE
            print(f"      Ort-Feld: {ort_name} = {DORTMUND_ORT_VALUE}")
        if umkreis_name:
            post_data[umkreis_name] = RADIUS_KM
            print(f"      Umkreis-Feld: {umkreis_name} = {RADIUS_KM} km")

    print(f"      POST → {action_url}")
    ergebnis_soup = soup_post(session, action_url, post_data)
    if not ergebnis_soup:
        return []

    alle_stellen = []
    seite = 1

    while ergebnis_soup and seite <= 20:
        stellen, naechste = parse_ergebnisse(ergebnis_soup, kategorie_id)
        alle_stellen.extend(stellen)
        print(f"      Seite {seite}: {len(stellen)} Treffer")

        if not naechste or naechste == form_url:
            break
        seite += 1
        time.sleep(1.5)
        ergebnis_soup = soup_get(session, naechste)

    return deduplizieren(alle_stellen)


def scrape_einfach(session, url, kategorie_id, label):
    """Für Kategorien ohne Suchformular (direkte Listenansicht)."""
    print(f"\n  📂 {label} (ganz NRW)")

    # Erstmal prüfen ob Formular oder direkte Liste
    soup = soup_get(session, url)
    if not soup:
        return []

    form = soup.find("form")
    if form:
        # Hat Formular → ohne Ortsfilter absenden
        return scrape_mit_formular(session, url, kategorie_id, mit_radius=False)
    else:
        # Direkte Liste
        stellen, naechste = parse_ergebnisse(soup, kategorie_id)
        print(f"    Seite 1: {len(stellen)} Treffer")
        seite = 1
        while naechste and seite <= 20:
            seite += 1
            time.sleep(1.5)
            soup2 = soup_get(session, naechste)
            if not soup2:
                break
            mehr, naechste = parse_ergebnisse(soup2, kategorie_id)
            stellen.extend(mehr)
            print(f"    Seite {seite}: {len(mehr)} Treffer")
        return deduplizieren(stellen)


# ── Hauptprogramm ─────────────────────────────────────────────────────────────

def main():
    print(f"\n🔍 STELLA NRW Scraper v3 — {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    try:
        session, kat_links = setup_session()
    except RuntimeError as e:
        print(f"❌ Fehler beim Setup: {e}")
        return

    alle_stellen = []

    # ── Schulstellen (Dortmund + 50 km) ──────────────────────────────────────
    if "schulbereich_url" in kat_links:
        stellen = scrape_schulbereich(session, kat_links["schulbereich_url"])
        print(f"  → {len(stellen)} Schulstellen im Radius")
        alle_stellen.extend(stellen)
    else:
        print("  ⚠ schulbereich_url nicht gefunden")

    time.sleep(2)

    # ── ZfsL / Fachleiter (Dortmund + 50 km) ─────────────────────────────────
    if "zfsl_url" in kat_links:
        print(f"\n  📂 ZfsL / Fachleiter (mit Radius Dortmund 50km)")
        stellen = scrape_mit_formular(
            session, kat_links["zfsl_url"],
            kategorie_id="zfsl",
            mit_radius=True,
        )
        print(f"  → {len(stellen)} ZfsL-Stellen im Radius")
        alle_stellen.extend(stellen)
    else:
        print("  ⚠ zfsl_url nicht gefunden")

    time.sleep(2)

    # ── Schulaufsicht (ganz NRW) ──────────────────────────────────────────────
    if "schulaufsicht_url" in kat_links:
        stellen = scrape_einfach(
            session, kat_links["schulaufsicht_url"],
            kategorie_id="schulaufsicht",
            label="Schulaufsicht / QUA-LiS / LAQUILA",
        )
        print(f"  → {len(stellen)} Schulaufsicht-Stellen")
        alle_stellen.extend(stellen)
    else:
        print("  ⚠ schulaufsicht_url nicht gefunden")

    time.sleep(2)

    # ── Sonstige Tätigkeiten (ganz NRW) ──────────────────────────────────────
    if "sonstige_url" in kat_links:
        stellen = scrape_einfach(
            session, kat_links["sonstige_url"],
            kategorie_id="sonstige",
            label="Sonstige Tätigkeiten",
        )
        print(f"  → {len(stellen)} Sonstige Stellen")
        alle_stellen.extend(stellen)
    else:
        print("  ⚠ sonstige_url nicht gefunden")

    # ── Ausgabe ───────────────────────────────────────────────────────────────
    os.makedirs("docs", exist_ok=True)

    output = {
        "aktualisiert":        datetime.now().isoformat(),
        "aktualisiert_lesbar": datetime.now().strftime("%d.%m.%Y um %H:%M Uhr"),
        "gesamt":              len(alle_stellen),
        "stellen":             alle_stellen,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Fertig! {len(alle_stellen)} Stellen → '{OUTPUT_FILE}'")
    print(f"   Zeitstempel: {output['aktualisiert_lesbar']}")

    # Kurze Zusammenfassung
    for kat in ["schulstellen", "zfsl", "schulaufsicht", "sonstige"]:
        n = sum(1 for s in alle_stellen if s["kategorie"] == kat)
        print(f"   {kat}: {n}")


if __name__ == "__main__":
    main()
