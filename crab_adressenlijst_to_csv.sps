* Encoding: windows-1252.
* stap 1 download crab adressenlijst.
* stap 2: open in QGIS en bereken de x en de y coordinaat (2 cijfers na de komma!)
* stap 3: doe een spatial join met Saga binnen QGIS : Processing>Toolbox>Saga>Add polygon attributes to points
* open hier.

GET TRANSLATE 
  FILE='C:\Users\plu3532\Documents\crab\CRAB_adressenlijst\Shapefile\crab_nis9.dbf' 
  /TYPE=DBF /MAP . 
DATASET NAME adressenlijst WINDOW=FRONT. 
delete variables d_r.
rename variables id = crab_id.
rename variables (straatnmid
straatnm
huisnr
apptnr
busnr
hnrlabel
niscode
gemeente
postcode
herkomst
x
y
nis9
=
straatnmid_adreslijst
straatnm_adreslijst
huisnr_adreslijst
apptnr_adreslijst
busnr_adreslijst
hnrlabel_adreslijst
niscode_adreslijst
gemeente_adreslijst
postcode_adreslijst
herkomst_adreslijst
x_adreslijst
y_adreslijst
statsec_adreslijst).

if crab_id > 2000000000 hoofdadres_id = crab_id - 2000000000.
if crab_id > 1000000000 & crab_id < 2000000000 subadres_id = crab_id - 1000000000.
EXECUTE.

sort cases crab_id (a).


SAVE OUTFILE='C:\Users\plu3532\Documents\crab\CRAB_adressenlijst\crab_adressenlijst.sav'
  /COMPRESSED.
