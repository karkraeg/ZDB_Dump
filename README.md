# Create ZDB Dump

`Create ZDB Dump` ist ein Script, um den 2x jährlich aktualiserten Dump der ZDB mit Daten über die OAI Schnittstelle zu aktualisieren.

Der Aufruf benötigt nach der Installation der benötigeten Pakete (`pip install -r requirements.txt`) entweder den Pfad zu einem lokal vorliegenden Dump oder die URL, bspw. https://opendata:opendata@data.dnb.de/zdb_lds_20220228.rdf.gz :

```bash
python createZDB_dump.py https://opendata:opendata@data.dnb.de/zdb_lds_20220228.rdf.gz
```

Das Script lädt dann entweder den Dump herunter oder nimmt den lokal vorliegenden Dump. Aus dem Dateinamen des Dumps wird das Datum abgeleitet, ab wann aktualisiert werden muss. Der Dump wird dann geparst und in ein Dictionary geschrieben. Über https://services.dnb.de/oai/repository?verb=ListRecords&metadataPrefix=RDFxml&set=zdb&from=2022-02-28 (das Datum aus dem Dump) werden dann alle seit dem 28. Februar aktualisierten Daten über OAI gezogen. In einem Abgleich werden aktualisierte Einträge überschrieben und neue hinzugefügt. Dabei kommt `//oai:record//rdf:Description@rdf:about` als Identifier des Datensatzes zum Einsatz un `//oai:record//rdf:Description//dcterms:modified` als Quelle des Modifikationsdatums des Eintrags.

Nach dem Zusammenführen der Datenquellen wird der neue Dump serialisiert und wieder komprimiert und mit dem aktuellen Datum versehen. So muss beim nächsten Update dann nur noch das Delta zwischen dem letzten Update und dem jeweiligen Datum über OAI aktualisiert werden.