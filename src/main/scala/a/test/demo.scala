package a.test

import java.io.IOException

/**
  * Created by mayn on 2018/3/5.
  */
object demo {
  def main(args: Array[String]) {
    var i = 0
      while(true){
         i = i + 1
         if (i == 10){
//           System.exit(1)
           throw new IOException()
         }else {
           println( i )
         }

      }
  }
}
