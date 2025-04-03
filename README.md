
# COSE  
**Continuous Shape Extraction from Dynamic Knowledge Graphs**

##  Build:  

   ```bash
   gradle build
   gradle shadowJar

   ```
   
## Run:

```bash
java -jar jar/COSE.jar config.properties > dbpedia.logs
```

## Configuration

Update the `config.properties` file with paths and values to your environment:

```properties
dataset_path=/path/to/dataset.nt
originalGraphPath=/path/to/originalGraph.nt
SHAPES_FILE_PATH=/path/to/shapes.ttl
output_file_path=/path/to/output/
graph_data_path=/path/to/graph_data.kryo
```

