* Encoding: windows-1252.

* check encoding
* download versie dbase vanop https://download.agiv.be/Producten/Detail?id=102
* opgelet: na unzippen 13 gig!

* pak de relaties
* hang de objecten eraan
* maak keuzes
* breng naar huisnummers.
* voeg sectoren toe.
* sla op.

DOELSTELLING
- huisnummers (en busnummers?) met één XY-coordinaat en een statistische sector
- ook gehistoriseerde huisnummers, toch als er geen nieuwe versie van is
- coordinaat bij voorkeur door gemeente, dan GRB gebouw, dan GRB perceel, dan andere bron

HUISNUMMER > hou recentste versie over
> verrijk met XY
>> open terreinobject huisnummer
>>> verreik met terreinobject


* ADRESPOSITIES.

GET TRANSLATE 
  FILE='C:\Users\plu3532\Documents\crab\CRAB_adresposities\dBASE\terrobj.dbf' 
  /TYPE=DBF /MAP . 
DATASET NAME terrobj WINDOW=FRONT.
delete variables d_r.
VALUE LABELS aard 
1 'kadastraal perceel'
2 'GRB gebouw'
3 'GRB kunstwerk'
4 'GRB perceel'
5 'geschetst gebouw'
99 'andere'.

* verwijder beïndigde obj indien er nog recente koppelingen zijn.
if einddatum>0 beindigde_obj=1.
AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=objid aard
  /beindigde_objen=SUM(beindigde_obj)
  /alle_objen=N.
compute behouden=1.
if beindigde_obj=1 & beindigde_objen<alle_objen behouden=0.
FILTER OFF.
USE ALL.
SELECT IF (behouden=1).
EXECUTE.
delete variables beindigde_objen alle_objen behouden.

* als er iets misloopt, check of terreinobj wel uniek zijn.

rename variables id=terrobjid.
rename variables (BEGINDATUM
BEGINTIJD
BEGINORG
BEGINBEW
EINDDATUM
=
BEGINDATUM_terrobj
BEGINTIJD_terrobj
BEGINORGANISATIE_terrobj
BEGINBEWERKING_terrobj
EINDDATUM_terrobj).

rename variables (
aard
kadgemcode
=
aard_terrobj
kadgemcode_terrobj).

sort cases terrobjid (a).
delete variables kadgemcode_terrobj.


* relatietabel met huisnummers.

GET TRANSLATE
  FILE='C:\Users\plu3532\Documents\crab\CRAB_adresposities\dBASE\tobjhnr.dbf'
  /TYPE=DBF /MAP .
DATASET NAME tobjhnr WINDOW=FRONT.
delete variables d_r.

* verwijder beïndigde koppelingen indien er nog recente koppelingen zijn.
if einddatum>0 beindigde_koppeling=1.
AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=huisnrid
  /beindigde_koppelingen=SUM(beindigde_koppeling)
  /alle_koppelingen=N.

compute behouden=1.
if beindigde_koppeling=1 & beindigde_koppelingen<alle_koppelingen behouden=0.
FILTER OFF.
USE ALL.
SELECT IF (behouden=1).
EXECUTE.
delete variables beindigde_koppelingen alle_koppelingen behouden.


rename variables id=tobjhnrid.
rename variables (BEGINDATUM
BEGINTIJD
BEGINORG
BEGINBEW
EINDDATUM
=
BEGINDATUM_tobjhnrid
BEGINTIJD_tobjhnrid
BEGINORGANISATIE_tobjhnrid
BEGINBEWERKING_tobjhnrid
EINDDATUM_tobjhnrid).


* verrijk met terrobj en sluit dataset.
sort cases terrobjid (a).
DATASET ACTIVATE tobjhnr.
MATCH FILES /FILE=*
  /TABLE='terrobj'
  /BY terrobjid.
EXECUTE.
dataset close terrobj.

* kies "de beste" positie.
recode aard_terrobj
(1=4)
(2=2)
(3=5)
(4=3)
(5=1)
(99=5) into voorkeur_aard.

recode einddatum_terrobj (missing=1) (else=0) into bestaand_terrobj.

* sorteer per huisnr, met op de eerste plaats de bestaande, of indien niets bestaat, het recentste, of indien alles bestaat op basis van voorkeur aard.
sort cases huisnrid (a) bestaand_terrobj (a) einddatum_terrobj (d) voorkeur_aard (a).

DATASET DECLARE xy.
AGGREGATE
  /OUTFILE='xy'
  /BREAK=huisnrid
  /x=FIRST(x) 
  /y=FIRST(y) 
  /objid=FIRST(objid) 
  /aard_terrobj=FIRST(aard_terrobj).
dataset activate xy.

dataset close tobjhnr.

* even checken of effectief GRB gebouwen het vaakste voorkomen.
FREQUENCIES VARIABLES=aard_terrobj
  /ORDER=ANALYSIS.



* huisnummers ophalen.

GET TRANSLATE
  FILE='C:\Users\plu3532\Documents\crab\CRAB_adresposities\dBASE\huisnr.dbf'
  /TYPE=DBF /MAP .
DATASET NAME huisnr WINDOW=FRONT.
delete variables d_r.

* hou enkel historische adressen over voor zover deze geen huidige variant meer hebben.
* Identify Duplicate Cases.
SORT CASES BY STRAATNMID(A) huisnr(A) EINDDATUM(A).
MATCH FILES
  /FILE=*
  /BY STRAATNMID huisnr
  /FIRST=PrimaryFirst
  /LAST=PrimaryLast.
DO IF (PrimaryFirst).
COMPUTE  MatchSequence=1-PrimaryLast.
ELSE.
COMPUTE  MatchSequence=MatchSequence+1.
END IF.
LEAVE  MatchSequence.
FORMATS  MatchSequence (f7).
COMPUTE  InDupGrp=MatchSequence>0.
SORT CASES InDupGrp(D).
MATCH FILES
  /FILE=*
  /DROP=PrimaryLast InDupGrp MatchSequence.
EXECUTE.

FILTER OFF.
USE ALL.
SELECT IF (PrimaryFirst = 1).
EXECUTE.
delete variables primaryfirst.

compute beeindigd_hoofdadres=0.
if einddatum>0 beeindigd_hoofdadres=1.

rename variables id=huisnrid.
rename variables (BEGINDATUM
BEGINTIJD
BEGINORG
BEGINBEW
EINDDATUM
=
BEGINDATUM_huisnummer
BEGINTIJD_huisnummer
BEGINORGANISATIE_huisnummer
BEGINBEWERKING_huisnummer
EINDDATUM_huisnummer).


* verrijk met coordinaten.
sort cases huisnrid (a).
DATASET ACTIVATE huisnr.
MATCH FILES /FILE=*
  /TABLE='xy'
  /BY huisnrid.
EXECUTE.

dataset close xy.


* mooi tussentijds resultaat.
SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\huisnrxy.sav'
  /COMPRESSED.


compute volgnummer=$casenum.
AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=x y
  /volgnummer_first=FIRST(volgnummer).



DATASET DECLARE geocode.
AGGREGATE
  /OUTFILE='geocode'
  /BREAK=volgnummer_first
  /x_first=FIRST(x) 
  /y_first=FIRST(y).
dataset activate geocode.


* zie bijhorend document om je SPSS-installatie hier klaar voor te maken: https://github.com/joostschouppe/Spatial-Join-in-SPSS/
* werkt enkel als je een SPSS versie hebt die met Python kan werken.
* we werken hier met de open data shapefile van de statistische sectoren, zoals verdeeld door statbel. Dat is niet zo leuk,
omdat er betere geometireën bestaan van de gemeentegrenzen. Dit is echter de beste publiek beschikbare versie van statsec.

* definieer programma.
BEGIN PROGRAM Python.
# Building a function to assign a point to a polygon
# dit vereist dat je SPSS versie een Python versie heeft die shapefile, rtree en shapely kent
import shapefile
from rtree import index
from shapely.geometry import Polygon, Point

# kies je shapefile: eerst de map, in de regel erna het bestand
# de eerste betekenisvolle kolom wordt genomen als identificator. Hier is dat de kolom waar vb A01-A1 in staat 
# (en waarmee je dus kan hercoderen naar de juiste buurt en wijk)
# de naam bg_statsec is willekeurig, en je hoeft die dus niet aan te passen met een andere bron
# je polygonen moeten wel steeds wederzijds uitsluitend zijn (dus een punt kan steeds slechts in een enkel gebied liggen)
data = r'C:\Users\plu3532\Documents\gebiedsindelingen\adsei_statsec'
bg_statsec = shapefile.Reader(data + r'\scbel01012011_gen13.shp')
bg_shapes = bg_statsec.shapes()  #convert to shapely objects
bg_points = [q.points for q in bg_shapes]
polygons = [Polygon(q) for q in bg_points]

#looking at the fields and records
bg_fields = bg_statsec.fields
bg_records = bg_statsec.records()
print bg_records[0][1] #als je hier commentaar uitzet, dan laat je de eerste record zien

#build spatial index from bounding boxes
#also has a second vector associating
#area IDs to numeric id
idx = index.Index()
c_id = 0
area_match = []
for a,b in zip(bg_shapes,bg_records):
    area_match.append(b[1])
    idx.insert(c_id,a.bbox,obj=b[1])
    c_id += 1

#now can define function with polygons, area_match, and idx as globals
def assign_area(x,y):
    if x == None or y == None: return None
    point = Point(x,y)
    for i in idx.intersection((x,y,x,y)):
        if point.within(polygons[i]):
            return area_match[i]
    return None
#note points on the borders will return None
        
END PROGRAM.

dataset activate geocode.

* enkel indien je wil kunnen timen.
SHOW $VAR.
* roep het programma op, zeg welke variabele opgevuld moet worden (hier statsec_splits) en hoeveel tekens deze mag hebben.
* type=0 betekent numeriek, type=9 een string van 9 tekens.
* geef vervolgens aan in welke variabele x en y teruggevonden kunnen worden.
SPSSINC TRANS RESULT=statsec TYPE=9
  /FORMULA "assign_area(x=x_first,y=y_first)".
SHOW $VARS.

SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\xystatsec.sav'
  /COMPRESSED.



dataset activate huisnr.
sort cases volgnummer_first (a).

MATCH FILES /FILE=*
  /TABLE='geocode'
  /BY volgnummer_first.
EXECUTE.
dataset close geocode.
delete variables volgnummer
volgnummer_first
x_first
y_first.

* opgelet: in de toekomst moet je wellicht koppelen, niet met substring werken.
string geostatsec_niscode (a5).
compute geostatsec_niscode=statsec.

dataset close geocode.

* verrijken met:
- straatnaam. Enige manier tot nu toe gevonden om DBF uit een GIS-omgeving goed in te lezen:
* open DBF in QGIS (normaal gezien worden tekens als ë nu wél goed ingelezen)
* sla op als CSV in system encoding
* open in SPSS met Unicode:OFF.




PRESERVE.
 SET DECIMAL COMMA.

GET DATA  /TYPE=TXT
  /FILE="C:\Users\plu3532\Documents\crab\CRAB_adresposities\straatnm.csv"
  /DELCASE=LINE
  /DELIMITERS=","
  /ARRANGEMENT=DELIMITED
  /FIRSTCASE=2
  /DATATYPEMIN PERCENTAGE=95.0
  /VARIABLES=
  ID AUTO
  NISGEMCODE AUTO
  STRAATNM AUTO
  TAALCODE AUTO
  STRAATNM2 AUTO
  TAALCODE2 AUTO
  BEGINDATUM AUTO
  EINDDATUM AUTO
  BEGINTIJD AUTO
  BEGINORG AUTO
  BEGINBEW AUTO
  STRAATNM0 AUTO
  /MAP.
RESTORE.

CACHE.
EXECUTE.
DATASET NAME straatnamen WINDOW=FRONT.

rename variables (
id
nisgemcode
straatnm
straatnm0
einddatum
=
straatnmid
niscode_crabstraatnaam
straatnaam_crabintern
straatnaam_crableesbaar
straatnaam_crabeinddatum).

match files
/file=*
/keep=straatnmid
niscode_crabstraatnaam
straatnaam_crabintern
straatnaam_crableesbaar
straatnaam_crabeinddatum.
sort cases straatnmid (a).





* toevoegen straatnamen, en ook CRAB-gemeente.
dataset activate huisnr.
sort cases straatnmid (a).
MATCH FILES /FILE=*
  /TABLE='straatnamen'
  /BY straatnmid.
EXECUTE.


SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\volledige_set.sav'
  /COMPRESSED.


* grootte van het mismatch-probleem meten.
DATASET DECLARE mismatch. 
AGGREGATE 
  /OUTFILE='mismatch' 
  /BREAK=geostatsec_niscode niscode_crabstraatnaam 
  /N_BREAK=N.


DATASET ACTIVATE mismatch.
alter type niscode_crabstraatnaam (a5).
if niscode_crabstraatnaam=geostatsec_niscode type=1.
if niscode_crabstraatnaam~=geostatsec_niscode & geostatsec_niscode="" type=2.
if niscode_crabstraatnaam~=geostatsec_niscode & geostatsec_niscode~="" type=3.
value labels type
1 'OK'
2 'geen geo-gemeente'
3 'verschillende geo-en crab-gemeente'.
* Custom Tables.
CTABLES
  /VLABELS VARIABLES=type N_BREAK DISPLAY=LABEL
  /TABLE type BY N_BREAK [SUM]
  /CATEGORIES VARIABLES=type ORDER=A KEY=VALUE EMPTY=INCLUDE
  /CRITERIA CILEVEL=95.
  
  * verdere afwerking nog nodig!
  * OPGELET: dit omvat gemeenten buiten Vlaanderen!
  
SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\mismatch.sav'
  /COMPRESSED.

dataset activate huisnr.
alter type niscode_crabstraatnaam (a5).
if niscode_crabstraatnaam=geostatsec_niscode type=1.
if niscode_crabstraatnaam~=geostatsec_niscode & geostatsec_niscode="" type=2.
if niscode_crabstraatnaam~=geostatsec_niscode & geostatsec_niscode~="" type=3.
value labels type
1 'OK'
2 'geen geo-gemeente'
3 'verschillende geo-en crab-gemeente'.



match files
/file=*
/keep=huisnrid
statsec
x
y
straatnaam_crabintern
straatnaam_crableesbaar
straatnmid
huisnr
geostatsec_niscode
niscode_crabstraatnaam
type.

rename variables (huisnrid
statsec
x
y
straatnaam_crabintern
straatnaam_crableesbaar
straatnmid
huisnr
geostatsec_niscode
niscode_crabstraatnaam
type=
geocode_huisnrid
geocode_statsec
geocode_x
geocode_y
geocode_straatnaam_crabintern
geocode_straatnaam_crableesbaar
geocode_straatnaam_id
geocode_crabhuisnr
geocode_niscode_statsec
geocode_niscode_crabintern
geocode_typematch).
string geocode_statsec_clean (a9).
if geocode_statsec="" geocode_statsec_clean=concat(geocode_niscode_crabintern,"ZZZZ").
if geocode_statsec="" & geocode_niscode_crabintern="" geocode_statsec_clean="99999ZZZZ".
if geocode_statsec~="" & geocode_niscode_crabintern=geocode_niscode_statsec geocode_statsec_clean=geocode_statsec.
if geocode_statsec~="" & geocode_niscode_crabintern~=geocode_niscode_statsec & geocode_niscode_statsec~="" geocode_statsec_clean=concat(geocode_niscode_crabintern,"ZZZZ").
EXECUTE.

sort cases geocode_huisnrid (a).


SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\koppel_statsec.sav'
  /COMPRESSED.

