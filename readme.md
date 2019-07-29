### Blocking Algorithm for Web Entities - Entity Resolution
   
---

Tasks:

- [x] Read RDF data from local file 
 
- [x] Job 1: Attribute Creation

- [x] Job 2: Attribute Similarities

- [ ] Job 3: Best Match

- [ ] Job 4: Final Clustering and Blocking

----

### Steps

**Data Preparation**
- Prepare 2 dataset (LOCAH and BPPedia)
- Convert link into text predicate (Using regexp. For example "<http://xmlns.com/foaf/0.1/givenname>" => "givenname")
- Give 3triple into format (datasetI-predicate, object) (datasetId: (0,1), predicate: text, object: text)
    Example: (1givenname, abla necroman)

**Job-1: Attribute Creation**
- Get data from preparetation step, convert into RDD
- Map phase: convert data into RDD format `(key: dId-predicate, value: object)` (dId: (0,1), predicate: Strng, object: String)
- Reduce phase: Concatnation all object by key, get all trigram, return RDD `(Key: dId-predicate, value: Set(trigram: String))`

Ex
- [1] (0-event, Set(ath, tho, hor,...))
- [2] (1-events, Set(ath, tho, ohr,...))
- ...

**Job-2: Attribute Similarity**
- Get RDD from previous step
- Use `flatmap()` to create multiple pairs for per map worker(Spark dont have Mapper-id purely, so i used partition-id).
    
    Example: we assume that 3 partition at all. 
    So partition 1 will create 3 pairs with keys [1-1, 1-2, 1-3].

- Use join by key to create pairs per Mapper. 
    
    Example: for key [1-1] we have value (a), (b), (c).
    This step return ([1-1], ((a), (b))), ([1-1], ((b), (c))), ([1-1], ((a), (c))) (~Reduce phase in Hadoop)
- Compare similarity.
    Example: ([1-1], ((name, Set(aaa, bbb, ccc)), (givenname, Set(aaa, bbb, ddd))))   
    Similarity = (name, (givenname, 0.5))

**Job-3: Bestmatch**
- Create (a, (b, similarity-of-a-b)) =>(b, (a, similarity-of-a-b)) for all result of sim 
- Join all same predicate, choose maximum similarity.

**Job-4: Blocking**

- Create clusters from predicates.

    For example, best match pairs:
    a-b
    b-c
    c-b
    m-n
    n-m
    
    => (a,b,c), (m,n) is clusters

- Create Block by token for per cluster.
- Find duplicate by Jaccard similarity.
  
  For example:
    
    e1: block1, block2, block3
    
    e2: block1, block2, block4
    
    e1.match(e2) = 0.5 => duplicate
 
- Remove duplicates (kept 1 for each duplicate)
- Save data to dataframe by SparkSql, then save to parquet format.

(https://spark.apache.org/docs/latest/sql-data-sources-parquet.html -> use this docs to read and write parquet dataframe)
