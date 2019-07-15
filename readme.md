### Blocking Algorithm for Web Entities - Entity Resolution
   
---

Tasks:

- [x] Read RDF data from local file 
 
- [x] Job 1: Attribute Creation

- [ ] Job 2: Attribute Similarities

- [ ] Job 3: Best Match

- [ ] Job 4: Final Clustering and Blocking

----

### Steps

**Data Preparation**
- Prepare 2 dataset (LOCAH and BPPedia)
- Convert link into text predicate (Using regexp. For example "<http://xmlns.com/foaf/0.1/givenname>" => "givenname")
- Give 3triple into format (datasetId;;;predicate;;;object) (datasetId: (0,1), predicate: text, object: text)

**Job-1: Attribute Creation**
- Get data from preparetation step, convert into RDD
- Map phase: convert data into RDD format `(key: dId-predicate, value: object)` (dId: (0,1), predicate: Strng, object: String)
- Reduce phase: Concatnation all object by key, get all trigram, return RDD `(Key: dId-predicate, value: Set(trigram: String))`

Ex
- [1] (0-event, Set(ath, tho, hor,...))
- [2] (1-events, Set(ath, tho, ohr,...))
- ...




