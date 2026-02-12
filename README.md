# Projekt – Coding-Aufgaben SVA

Dieses Repository dokumentiert mein **Projekt** im Rahmen des Vorstellungsgesprächs bei **SVA**
Ziel des Projekts war es, anhand realer Tankstellen- und Preisdaten analytische Fragestellungen strukturiert, reproduzierbar und datengetrieben zu lösen.

Der Fokus lag auf:
- sauberer Datenanalyse,
- Validierung und Plausibilitätsprüfungen,
- nachvollziehbarer Projektstruktur,
- sowie dem Einsatz moderner Cloud- und Big-Data-Technologien.

---

## 📁 Projektstruktur

```text
Projekt – Github klafi11
│
├ Aufgabe 1 – Südlichste Tankstelle
│   └ Eigenständiges Projekt (uv)
│
├ Aufgabe 2 – Höchster E10-Preis
│   └ Eigenständiges Projekt (uv)
│
├ Aufgabe 3 – Niedrigster Diesel-Preis
│   └ Eigenständiges Projekt (uv)

```
Jede Aufgabe wurde als eigenständiges Projekt mit uv umgesetzt.

## Aufgabe 1 - Welches ist die südlichste Tankstelle Deutschlands?

### Zielsetzung 

Bestimmung der südlichsten Tankstelle Deutschlands anhand geografischer Koordinaten.

### Rahmenbedingungen

- Betrachtungszeitraum: 30 Tage, ausgehend vom 10.02.2026
- Nutzung relevanter Stationstabellen mit Längen- und Breitengradangaben
- Bestimmung über den Breitengrad, da dieser die Nord-Süd-Position definiert

### Vorgehen 

- Systematische Analyse der Stationstabellen
- Bereinigung fehlerhafter Geo-Daten
- Identifikation des niedrigsten Breitengrads
- Aufbau einer strukturierten Suchabfrage
- Validierung auffälliger Ergebnisse (z. B. Ausschluss von Einträgen aus Köln und Österreich)

### Ergebnis

Nach Validierung der Daten gilt die Shell Tankstelle in Mittelwald (47,399570° N) als die südlichste Tankstelle Deutschlands innerhalb der gegebenen Datenbasis.

---

## Aufgabe 2 - Wie hoch war 2023 der höchste Preis für E10?

### Zielsetzung

Ermittlung des höchsten E10-Preises im Jahr 2023 unter Berücksichtigung von Datenqualität und Plausibilität

### Rahmenebdigungen

- 365 tägliche Preistabellen aus 2023
- ca. 30 MB pro Datei (≈ 10 GB Gesamtdaten)
- Datenquelle: Git-Repository Tankerkönig

### Infrastruktur

- Sparse Checkout (Partial Clone) des Repositories
- Infrastrukturaufbau mit Terraform (OpenTofu) in der Google Cloud
- Cloud Storage Bucket + Service Account inkl. IAM-Rollen
- Einsatz von Dataproc Serverless (Apache Spark)
- Batch-Workflow zur Preisfilterung
- Lokale Tests mit Pytest

### Vorgehen

- Upload der Preistabellen in ein bucket-raw Verzeichnis
- Auslesen aller 2023-Daten mit Spark
- Bereinigung:
    - Preis = 0
    - Null-Values
- Ermittlung des maximalen E10-Preises
- Validierung durch Schwellenwert-Logik (≤ 3,00 €)

### Ergebnis

- Ohne Schwellenwert: 6 Datensätze mit unplausiblen Preisen zwischen 4,79 € – 4,99 € → vermutlich Eingabefehler
- Mit Schwellenwert (≤ 3,00 €): ca. 150 valide Datensätze → realistischer E10-Preis: 2,99 €

## Aufgabe 3 - Wo gab es vorgestern den güstigsten Diesel?

### Zielsetzung 

Ermittlung der Tankstelle mit dem niedrigsten Dieselpreis zum Stichtag 11.02.2026.

### Rahmenbedingungen
 
- Preis & Stationstabelle zum Stichtag

### Vorgehen

- Download der Preis- und Stationstabellen
- Zusammenführung über Stations-ID
- Bereinigung:
    - Preis = 0
    - Null-Values
- Filterung nach niedrigstem Dieselpreis
- Validierung über Top-3 niedrigste Preise

### Ergebnis

- Günstigster Dieselpreis: Budget Oil – Overath → jedoch nur eingeschränkt valide (veraltete Daten)
- Platz 2 & 3: Tankstellen in Kronach und Wilhelmshaven → realistischere Preise, jedoch auf geringer Datenbasis

## Fazit

Dieses Projekt zeigt:
- strukturiertes analytisches Vorgehen,
- saubere Datenbereinigung und Validierung,
- den sinnvollen Einsatz von Cloud- und Big-Data-Technologien,
- sowie ein kritisches Verständnis für Datenqualität und Plausibilität.

---

# English Version - Project – Coding Tasks SVA

This repository documents my project as part of the interview process at SVA.
The goal of the project was to solve analytical questions in a structured,
reproducible, and data-driven way using real gas station and fuel price data.

The focus was on:
- clean data analysis,
- validation and plausibility checks,
- a transparent and well-structured project setup,
- and the use of modern cloud and big data technologies.


## Project Structure

```text
Project – Github klafi11
|
|-- Task 1 – Southernmost Gas Station
|   |-- Independent project (uv)
|
|-- Task 2 – Highest E10 Price
|   |-- Independent project (uv)
|
|-- Task 3 – Lowest Diesel Price
|   |-- Independent project (uv)
```

Each task was implemented as an independent project using uv.


## Task 1 – Which is the southernmost gas station in Germany?

### Objective

Determine the southernmost gas station in Germany based on geographical coordinates.

### Constraints

- Observation period: 30 days starting from 10 February 2026
- Use of relevant station tables containing latitude and longitude data
- Determination based on latitude, as it defines the north–south position

### Approach

- Systematic analysis of station tables
- Cleaning of faulty geolocation data
- Identification of the lowest latitude value
- Construction of a structured query
- Validation of suspicious results (e.g. exclusion of entries from Cologne and Austria)

### Result

After data validation, the Shell gas station in Mittelwald (47.399570° N)
is considered the southernmost gas station in Germany within the given dataset.


## Task 2 – What was the highest E10 price in 2023?

### Objective

Determine the highest E10 fuel price in 2023 while considering data quality
and plausibility.

### Constraints

- 365 daily price tables from 2023
- Approx. 30 MB per file (≈ 10 GB total data volume)
- Data source: Tankerkönig Git repository

### Infrastructure

- Sparse checkout (partial clone) of the repository
- Infrastructure provisioning with Terraform (OpenTofu) on Google Cloud
- Cloud Storage bucket and service account including IAM roles
- Use of Dataproc Serverless (Apache Spark)
- Batch workflow for price filtering
- Local tests using Pytest

### Approach

- Upload of price tables into a bucket-raw directory
- Reading all 2023 data with Spark
- Data cleaning:
  - Price = 0
  - Null values
- Determination of the maximum E10 price
- Validation using threshold logic (≤ €3.00)

### Result

- Without threshold: 6 records with implausible prices between €4.79 – €4.99,
  likely caused by input errors
- With threshold (≤ €3.00): approximately 150 valid records,
  realistic E10 price: €2.99


## Task 3 – Where was the cheapest diesel price the day before yesterday?

### Objective

Identify the gas station with the lowest diesel price on the reference date
11 February 2026.

### Constraints

- Price and station tables for the given date

### Approach

- Download of price and station tables
- Joining via station ID
- Data cleaning:
  - Price = 0
  - Null values
- Filtering for the lowest diesel price
- Validation using the top three lowest prices

### Result

- Cheapest diesel price: Budget Oil – Overath,
  limited validity due to outdated data
- Rank 2 and 3: gas stations in Kronach and Wilhelmshaven,
  more realistic prices but based on a limited data foundation


## Conclusion

This project demonstrates:
- a structured analytical approach,
- clean data preparation and validation,
- the effective use of cloud and big data technologies,
- and a critical understanding of data quality and plausibility.
