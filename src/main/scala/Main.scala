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
import java.io.InputStream
import java.io.BufferedOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer

case class Config(out: String = null, schema: File = null, files: Seq[String] = Seq(), filter: Map[String,String] = Map())

object AvroFilter {
  val parser = new scopt.OptionParser[Config]("avro-filter") {
    head("avro-filter", "0.1")

    opt[Map[String,String]]('f', "filter").required().valueName("k1=v1,k2=v2...").action( (x, c) =>
      c.copy(filter = x) ).text("filter expression, eg. user_id=1")

    opt[String]('o', "out").required().valueName("<file>").
      action( (x, c) => c.copy(out = x) ).
      text("output file")

    opt[File]('s', "schema").valueName("<file>").
      action( (x, c) => c.copy(schema = x) ).
      text("optional schema to use when reading")

    help("help").text("prints this usage text")

    arg[String]("<files>...").unbounded().action( (x, c) =>
      c.copy(files = c.files :+ x) ).text("input file(s)")
  }

  def getFiles(fileOrDirName:String) : List[Path] = {
    val path:Path = new Path(fileOrDirName);
    val fs:FileSystem = path.getFileSystem(new Configuration())

    val fileList = new ListBuffer[Path]()

    if (fs.isFile(path)) {
      fileList += path
    }
    else if (fs.getFileStatus(path).isDir()) {
      for (status <- fs.listStatus(path)) {
        if(!status.isDir()) {
          fileList += status.getPath()
        }
      }
    }
    return fileList.toList
  }

  def openOutput(fileName:String) : OutputStream = {
    val p:Path = new Path(fileName)
    return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
  }

  def main(args: Array[String]) = {


    val options = parser.parse(args, Config())

    if (options == None) {
      System.exit(0)
    }


    var inFiles = List[Path]()
    for (path <- options.get.files) {
      inFiles = inFiles ++ getFiles(path)
    }

    var schema :Schema = null
    if (options.get.schema != null) {
      schema = (new Schema.Parser()).parse(options.get.schema)
      println("Using schema: " + schema.getFullName())
    }

    val writer :DataFileWriter[GenericRecord] = new DataFileWriter(new GenericDatumWriter())
    val codecFactory = CodecFactory.deflateCodec(9)
    writer.setCodec(codecFactory)

    val outFile = openOutput(options.get.out)
    var isOpen = false

    var writeCount:Int = 0
    var totalCount:Int = 0

    for (inFile:Path <- inFiles) {
      var reader:GenericDatumReader[GenericRecord] = null
      val input:InputStream = inFile.getFileSystem(new Configuration()).open(inFile)

      if (schema != null) {
        reader = new GenericDatumReader(schema)
      } else {
        reader = new GenericDatumReader()
      }

      val fileReader = new DataFileStream(input, reader)

      if (schema == null) {
        schema = fileReader.getSchema()
      }

      if (!isOpen) {
        writer.create(schema, outFile)
        isOpen = true
      }

      while (fileReader.hasNext()) {
        breakable {
          totalCount += 1
          val data = fileReader.next()
          options.get.filter.foreach { case(k, v) =>
            val value = data.get(k)
            if (value == null || value.toString != v) break
          }
          writeCount += 1
          writer.append(data)
        }
      }

      fileReader.close()
    }

    println(s"Wrote $writeCount of total $totalCount records")

    writer.flush()
    writer.close()
  }
}
