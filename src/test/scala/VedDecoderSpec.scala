import com.opentok.udfs.DecodeVed
import org.scalatest.{Matchers, WordSpec}

class VedDecoderSpec extends WordSpec with Matchers {

  val udf = new DecodeVed()

  "should decode a simple google analytics ved code" in {
    udf.Decoder("0CCkQFjAA").get shouldEqual
      Array(null, "normal_universal_search_result", 0, 0, "page_01")
  }

  "should decode a simple google analytics ved code with adword position" in {
    udf.Decoder("0CBsQ0QxqFQoTCNvfnuuD88YCFU8ziAodjs4JM").get shouldEqual
      Array(1, "adword", 0, 0, "page_01")
  }

//  "should decode a simple google analytics ved code with adword position in right panel" in {
//    udf.Decoder("0CCkQFjAA").get shouldEqual
//      Array(null, "normal_universal_search_result", 0, 0, "page_01")
//  }
//
//  "should decode a simple google analytics ved code on page two position one" in {
//    udf.Decoder("0CCkQFjAA").get shouldEqual
//      Array(null, "normal_universal_search_result", 0, 0, "page_01")
//  }
//
//  "should decode a simple google analytics ved code from image search" in {
//    udf.Decoder("0CCkQFjAA").get shouldEqual
//      Array(null, "normal_universal_search_result", 0, 0, "page_01")
//  }
}
