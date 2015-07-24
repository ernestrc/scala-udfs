import com.opentok.udfs.DecodeVed
import org.scalatest.{Matchers, WordSpec}

class VedDecoderSpec extends WordSpec with Matchers {

  val udf = new DecodeVed()

  "should decode a protobuf google analytics ved code" in {
    udf.Decoder("0CCkQFjAA") shouldEqual
      Array(null, "normal_universal_search_result", null, 1, null, null, null, null, null)
  }

  "should decode a plain text google analytics ved code" in {
    udf.Decoder("1t:429,r:65,s:500,i:199") shouldEqual
      Array("right_hand_column_or_bottom", "image_search_result", null, 66, "page_51", null, null, null, null)
  }

  "should decode a protobuf google analytics ved code with sponsored search result in right panel" in {
    udf.Decoder("0CLgBENEMahUKEwjbgPXsovTGAhXOLIgKHf0VDWU") shouldEqual
      Array("right_hand_column_or_bottom", "sponsored_search_result", null, null, "page_1", "2015-07-24 10:17:27.485", "2015-07-24",
        176696526, 1695356413)
  }

  "should decode a protobuf google analytics ved code with sponsored search result" in {
    udf.Decoder("0CBwQ0QxqFQoTCMu03cWi9MYCFQOYiAod9ycEqA") shouldEqual
      Array(null, "sponsored_search_result", null, null, "page_1", "2015-07-24 10:16:05.310", "2015-07-24", 176723971, -1476122633)
  }

  "should decode a protobuf google analytics ved code with sponsored search result in right hand panel at position 3" in {
    udf.Decoder("0CJUBENEMahUKEwjXn6-BsvTGAhUMfogKHQ1NDMU") shouldEqual
      Array("right_hand_column_or_bottom", "sponsored_search_result", null, null, "page_1", "2015-07-24 11:25:16.914", "2015-07-24",
        176717324, -989049587)
  }

  "should decode a protobuf google analytics ved code on page two position one" in {
    udf.Decoder("0CB0QFjAAOApqFQoTCMuWsaen9MYCFZdZiAod2dYIJA") shouldEqual
      Array(null, "normal_universal_search_result", null, 1, "page_2", "2015-07-24 10:37:23.848", "2015-07-24", 176707991, 604559065)
  }

  "should decode a protobuf google analytics ved code from image search in position 10 (13 - 3 paid search) page 10" in {
    udf.Decoder("0CG8QFjAJOFpqFQoTCLXI4ZKt9MYCFdItiAodWfEOoQ") shouldEqual
      Array("right_hand_column_or_bottom", "normal_universal_search_result", null, 10, "page_10",
        "2015-07-24 11:03:31.213", "2015-07-24", 176696786, -1592856231)
  }

  "should decode a protobuf google analytics ved code that starts with invalid characters" in {
    udf.Decoder("D0CB4Q0QxqFQoTCNXk6Km388YCFUEbFAodiOQGPg") shouldEqual
      Array(null, "sponsored_search_result", null, null, "page_1", "2015-07-24 02:16:24.182", "2015-07-24", 169089857, 1040639112)
  }

  "should return an array of null values if could not decode ved" in {
    udf.Decoder("aaaaaaaaaaaaa") shouldEqual Array(null, null, null, null, null, null, null, null, null)
    udf.Decoder("1aaaaaaaaaaaa") shouldEqual Array(null, null, null, null, null, null, null, null, null)
    udf.Decoder("0aaaaaaaaaaaa") shouldEqual Array(null, null, null, null, null, null, null, null, null)
    udf.Decoder("0aaaaaaaaaaaabbbbbbbbccccccc") shouldEqual Array(null, null, null, null, null, null, null, null, null)
  }
}