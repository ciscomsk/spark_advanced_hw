package org.apache.spark.sql.hybrid

import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec

object FileHelper {

  def ensureDirectory(path: String): Unit =
    this.synchronized {
      val dir: File = new File(path)
      dir.mkdir
    }

  def write(path: String, data: Iterator[String]): Unit = {
    val file: File = new File(path)
    val bw: BufferedWriter = new BufferedWriter(new FileWriter(file))

    @tailrec
    def iterTraversal[T](iter: Iterator[T]): Unit =
      if (iter.hasNext) {
        bw.write(data.next)
        bw.write("\n")

        iterTraversal(iter)
      }

    iterTraversal(data)
    bw.close()
  }


}
