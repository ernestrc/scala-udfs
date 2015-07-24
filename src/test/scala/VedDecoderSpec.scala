import com.opentok.udfs.DecodeVed
import org.scalatest.{Matchers, WordSpec}

class VedDecoderSpec extends WordSpec with Matchers {

  val udf = new DecodeVed()

  "should decode a protobuf google analytics ved code" in {
    udf.Decoder("0CCkQFjAA").get shouldEqual
      Array(null, "normal_universal_search_result", 1, 1, "page_1", 0, 0, 0)
  }

  "should decode a plain text google analytics ved code" in {
    udf.Decoder("1t:429,r:65,s:500,i:199").get shouldEqual
      Array("right_hand_column_or_bottom", "image_search_result", 1, 66, "page_51", 0, 0, 0)
  }

  "should decode a protobuf google analytics ved code with sponsored search result in right panel" in {
    udf.Decoder("0CLgBENEMahUKEwjbgPXsovTGAhXOLIgKHf0VDWU").get shouldEqual
      Array("right_hand_column_or_bottom", "sponsored_search_result", 1, 1, "page_1", 1437758247485531L,
        176696526, 1695356413)
  }

  "should decode a protobuf google analytics ved code with sponsored search result" in {
    udf.Decoder("0CBwQ0QxqFQoTCMu03cWi9MYCFQOYiAod9ycEqA").get shouldEqual
      Array(null, "sponsored_search_result", 1, 1, "page_1", 1437758165310027L, 176723971, -1476122633)
  }

  "should decode a protobuf google analytics ved code with sponsored search result in right hand panel at position 3" in {
    udf.Decoder("0CJUBENEMahUKEwjXn6-BsvTGAhUMfogKHQ1NDMU").get shouldEqual
      Array("right_hand_column_or_bottom", "sponsored_search_result", 1, 1, "page_1", 1437762316914647L,
        176717324, -989049587)
  }

  "should decode a protobuf google analytics ved code on page two position one" in {
    udf.Decoder("0CB0QFjAAOApqFQoTCMuWsaen9MYCFZdZiAod2dYIJA").get shouldEqual
      Array(null, "normal_universal_search_result", 1, 1, "page_2", 1437759443848011L, 176707991, 604559065)
  }

  "should decode a protobuf google analytics ved code from image search in position 3" in {
    udf.Decoder("0CB4QMygCMAJqFQoTCIHinOOp9MYCFcYyiAod1y8H0A").get shouldEqual
      Array(null, "normal_universal_search_result", 3, 3, "page_1", 1437760106213633L, 176698054, -804835369)
  }

  "should decode a protobuf google analytics ved code from image search in position 10 (13 - 3 paid search) page 10" in {
    udf.Decoder("0CG8QFjAJOFpqFQoTCLXI4ZKt9MYCFdItiAodWfEOoQ").get shouldEqual
      Array("right_hand_column_or_bottom", "normal_universal_search_result", 1, 10, "page_10",
        1437761011213365L, 176696786, -1592856231)
  }
}