SELECT BIRTHDATE, ID, PREFIX, FIRST, SUFFIX, LAST, GENDER, STATE,
(NVL(PREFIX,'')||' '||NVL(FIRST,'')||' '||NVL(LAST,'')||' '||NVL(SUFFIX,'')) AS FULLNAME
FROM SAMPLE_SRC_PATIENTS