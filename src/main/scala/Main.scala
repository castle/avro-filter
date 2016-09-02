import util.control.Breaks._

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.file.CodecFactory

import java.io.File
import java.io.FileInputStream

case class Config(out: File = new File("."), schema: File = null, files: Seq[File] = Seq(), filter: Map[String,String] = Map())

object AvroFilter {
  val parser = new scopt.OptionParser[Config]("avro-filter") {
    head("avro-filter", "0.1")

    opt[Map[String,String]]('f', "filter").required().valueName("k1=v1,k2=v2...").action( (x, c) =>
      c.copy(filter = x) ).text("filter expression, eg. user_id=1")

    opt[File]('o', "out").required().valueName("<file>").
      action( (x, c) => c.copy(out = x) ).
      text("output file")

    opt[File]('s', "schema").valueName("<file>").
      action( (x, c) => c.copy(schema = x) ).
      text("optional schema to use when reading")

    help("help").text("prints this usage text")

    arg[File]("<files>...").unbounded().action( (x, c) =>
      c.copy(files = c.files :+ x) ).text("input file(s)")
  }

  def main(args: Array[String]) = {
    val options = parser.parse(args, Config())

    if (options == None) {
      System.exit(0)
    }

    val file = options.get.files.head
    if ( !file.canRead() ) {
      println("File cannot be opened for reading")
      System.exit(1)
    }

    var reader :GenericDatumReader[GenericRecord] = null
    var schema :Schema = null
    if (options.get.schema != null) {
      schema = (new Schema.Parser()).parse(options.get.schema)
      reader = new GenericDatumReader(schema)
    } else {
      reader = new GenericDatumReader()
    }

    val fileStream = new FileInputStream(file)
    val fileReader = new DataFileStream(fileStream, reader)

    if (schema == null) {
      schema = fileReader.getSchema()
    }

    println("Using schema: " + schema.getFullName())
    val codecFactory = CodecFactory.deflateCodec(9)

    val writer :DataFileWriter[GenericRecord] = new DataFileWriter(new GenericDatumWriter())
    writer.setCodec(codecFactory)

    val outFile = options.get.out
    writer.create(schema, outFile)

    var writeCount:Int = 0
    var totalCount:Int = 0

    while (fileReader.hasNext()) {
      breakable {
        totalCount += 1
        val data = fileReader.next()
        options.get.filter.foreach { case(k, v) =>
          if (data.get(k).toString != v) break
        }
        writeCount += 1
        writer.append(data)
      }
    }

    println(s"Wrote $writeCount of total $totalCount records")

    writer.flush()
    writer.close()
  }
}
