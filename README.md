## avro-filter

Reads Avro files and writes all records that matches a filter expression to a new Avro file

Filters are specified using the `-f` parameter, eg.

```bash
java -jar avro-filter.jar -o out.avro -f user_id=1,status=failed transactions.avro
```

```bash
$ java -jar avro-filter.jar --help
avro-filter 0.1
Usage: avro-filter [options] <files>...

  -f, --filter k1=v1,k2=v2...
                           filter expression, eg. user_id=1
  -o, --out <file>         output file
  -s, --schema <file>      optional schema to use when reading
  --help                   prints this usage text
  <files>...               input file(s)
```


## TODO

- [ ] Handle multiple input files
- [ ] Split output file in configurable chunks (max size)
- [ ] Configurable compression options

## Development

run with

```bash
sbt "run -o out.avro -f user_id=1 -s schema.avro input.avro"
```

## Build

Build a JAR containg all dependencies
```bash
sbt assembly
```
