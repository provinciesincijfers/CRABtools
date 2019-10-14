* Encoding: windows-1252.


GET
  FILE='C:\Users\plu3532\Documents\crab\verwerkt\koppel_statsec.sav'.
DATASET NAME huisnummers WINDOW=FRONT.


DATASET DECLARE aggr.
AGGREGATE
  /OUTFILE='aggr'
  /BREAK=geocode_straatnaam_id geocode_statsec_clean
  /huisnummers=N.

dataset close huisnummers.

DATASET ACTIVATE aggr.
AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=geocode_straatnaam_id
  /totaal_huisnummers=sum(huisnummers)
  /opties=N.

compute kans=huisnummers/totaal_huisnummers.
EXECUTE.

* Identify Duplicate Cases.
SORT CASES BY geocode_straatnaam_id(A) kans(d).
MATCH FILES
  /FILE=*
  /BY geocode_straatnaam_id
  /FIRST=PrimaryFirst.
VARIABLE LABELS  PrimaryFirst 'Indicator of each first matching case as Primary'.
VALUE LABELS  PrimaryFirst 0 'Duplicate Case' 1 'Primary Case'.
VARIABLE LEVEL  PrimaryFirst (ORDINAL).
EXECUTE.

FILTER OFF.
USE ALL.
SELECT IF (PrimaryFirst = 1).
EXECUTE.
match files
/file=*
/keep=geocode_straatnaam_id
geocode_statsec_clean kans.
RENAME VARIABLES geocode_straatnaam_id = streetcode_straatnaam_id.
rename variables kans=streetcode_straatnaam_correctheid.
rename variables geocode_statsec_clean=streetcode_statsec_clean.

sort cases streetcode_straatnaam_id (a).


SAVE OUTFILE='C:\Users\plu3532\Documents\crab\verwerkt\koppel_statsec_op_straatnaam.sav'
  /COMPRESSED.
