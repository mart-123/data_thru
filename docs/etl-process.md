# ETL Process
This document describes the Extract, Transform, Load (ETL) pipeline for HESA student data.


Each delivery is processed through identical extract, load, and staging steps, with deliveries combined in the staging layer using UNION operations. This approach allows:

