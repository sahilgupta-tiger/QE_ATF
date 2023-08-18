SELECT src.BIRTHDATE as BIRTHDATE, src.ID as ID, src.PREFIX as PREFIX, src.PREFIX as PREFIX,
src.FIRST as FIRST, src.SUFFIX as SUFFIX, src.LAST as LAST, src.GENDER as GENDER, src.STATE as
STATE, NVL(src.PREFIX,'')||' '||NVL(src.FIRST,'')||' '||NVL(src.LAST,'')||' '||NVL(src.SUFFIX,'') as FULLNAME FROM
orc_patients_db src;