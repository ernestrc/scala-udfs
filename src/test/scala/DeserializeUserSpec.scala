import com.opentok.udfs.DeserializeUser
import org.scalatest._

class DeserializeUserSpec extends WordSpec with Matchers {

  val udf = new DeserializeUser()

  "should serialize and clean a user from a paper trail string" in {
    val mess = """---⇒email: foo@bar.com⇒encrypted_password: abc⇒reset_password_token: ⇒reset_password_sent_at: ⇒remember_created_at: ⇒sign_in_count: 27⇒current_sign_in_at: 2014-09-18 22:25:55.518630000 Z⇒last_sign_in_at: 2014-09-16 23:56:19.596348000 Z⇒current_sign_in_ip: 179.24.7.62⇒last_sign_in_ip: 186.53.100.68⇒confirmation_token: ⇒confirmed_at: 2014-08-26 15:48:00.897250000 Z⇒confirmation_sent_at: 2014-08-26 15:46:52.506607000 Z⇒unconfirmed_email: ⇒authentication_token: 5Zxwmo5a3ZmbU7TSNrcq⇒created_at: 2014-08-26 15:46:52.595783000 Z⇒updated_at: 2014-09-26 16:40:01.512446000 Z⇒name: Gaston⇒sso_token: debizsotnlwgijcyewcttimwiasspfohgnajsxli⇒username: ⇒customer_id: ⇒company_name: ''⇒company_url: ''⇒migrated: true⇒tour_complete: false⇒type: V2User⇒v1_usage_plan_id: ⇒v1_support_plan_id: ⇒trial_ends_at: 2014-10-25⇒converted_to_v2_at: ⇒v2_usage_plan_id: 1⇒v2_support_plan_id: 1⇒state: trialing⇒invoice_type: cc⇒developer: true⇒company_industry: ⇒address_zip: ⇒company_type: ⇒job_type: developer⇒qs_delivery: ⇒role: ⇒id: 157711⇒"""
    val u = udf.Deserializer.deserialize(mess).get

    u should contain theSameElementsInOrderAs Array(null, null, "2014-09-16 23:56:19.596348000 Z", 1,
      null, "Gaston", "5Zxwmo5a3ZmbU7TSNrcq", null, "abc", "cc", "''", null, "foo@bar.com", "trialing",
      null, null, null, "179.24.7.62", true, null, true, null, 1, 157711, "2014-08-26 15:48:00.897250000 Z", null,
      null, "2014-08-26 15:46:52.506607000 Z", 27, "186.53.100.68", "2014-08-26 15:46:52.595783000 Z",
      "debizsotnlwgijcyewcttimwiasspfohgnajsxli", false, "V2User", null, "2014-09-18 22:25:55.518630000 Z",
      "2014-09-26 16:40:01.512446000 Z", null, null, null, "developer", "2014-10-25", "''", null)
  }

  "should serialize and clean a user from a bogus paper trail string and ignore unknown keys" in {
    val mess = "reset_password_token: ⇒reset_password_sent_at: ⇒remember_created_at: ⇒sign_in_count: 4⇒current_sign_in_at: 2014-12-19 11:59:55.914684000 Z⇒last_sign_in_at: 2014-12-19 08:06:43.923069000 Z⇒current_sign_in_ip: 100.0.0.1⇒last_sign_in_ip: 100.0.0.1⇒confirmation_token: ⇒confirmed_at: 2014-06-02 06:43:53.760939000 Z⇒confirmation_sent_at: 2014-06-02 06:43:18.790954000 Z⇒unconfirmed_email: ⇒authentication_token: j9TRpYZo5xipPhzMQQ1h⇒created_at: 2014-06-02 06:43:18.884008000 Z⇒updated_at: 2014-12-19 11:59:55.922946000 Z⇒name: Özgür⇒sso_token: xr2kjxrimnacphizwvbenfgfemkhwgkxqqjwruch⇒username: ⇒customer_id: ⇒company_name: ÜRÜNLERİ⇒  A.Ş. BAYAR CAD.  NO:109  34742 KOZYATAĞI - İSTANBUL - TÜRKİYE İSTANBUL ANADOLU ANADOLU⇒  KURUMLAR / 8750213491⇒company_url: http://www.tumzzzzzzz.com⇒migrated: true⇒tour_complete: false⇒type: V2User⇒v1_usage_plan_id: ⇒v1_support_plan_id: ⇒trial_ends_at: 2014-07-01⇒converted_to_v2_at: ⇒v2_usage_plan_id: 1⇒v2_support_plan_id: 1⇒state: suspended⇒invoice_type: cc⇒developer: true⇒company_industry: ⇒address_zip: ⇒company_type: a company⇒job_type: director/executive⇒qs_delivery: self⇒role: ⇒id: 150824⇒"
    val u = udf.Deserializer.deserialize(mess).get

    u should contain theSameElementsInOrderAs Array("a company", null, 
      "2014-12-19 08:06:43.923069000 Z", 1, null, "Özgür", "j9TRpYZo5xipPhzMQQ1h", 
      null, null, "cc", "ÜRÜNLERİ", 
      null, null, "suspended", null, null, null, "100.0.0.1", true, null, true, null, 
      1, 150824, "2014-06-02 06:43:53.760939000 Z", null, null, "2014-06-02 06:43:18.790954000 Z", 4, 
      "100.0.0.1", "2014-06-02 06:43:18.884008000 Z", "xr2kjxrimnacphizwvbenfgfemkhwgkxqqjwruch", false, 
      "V2User", "self", "2014-12-19 11:59:55.914684000 Z", "2014-12-19 11:59:55.922946000 Z", null, null, null, 
      "director/executive", "2014-07-01", "http://www.tumzzzzzzz.com", null)
  }

  "should serialize a user from a paper trail string" in {
    val mess = """
                 |---
                 |email: foo@bar.com
                 |encrypted_password: xword
                 |reset_password_token:
                 |reset_password_sent_at:
                 |remember_created_at:
                 |sign_in_count: 0
                 |current_sign_in_at:
                 |last_sign_in_at:
                 |current_sign_in_ip:
                 |last_sign_in_ip:
                 |confirmation_token: XmpAAp11iydxH52a6MCB
                 |confirmed_at:
                 |confirmation_sent_at: 2012-10-24 10:29:55.228935000 Z
                 |unconfirmed_email:
                 |authentication_token: i5U4AvtSv1RW2MkKxC7q
                 |created_at: 2012-10-24 10:29:55.180866000 Z
                 |updated_at: 2015-01-09 06:39:14.200373835 Z
                 |name:
                 |sso_token:
                 |username: chin
                 |customer_id:
                 |company_name: ''
                 |company_url: ''
                 |migrated: true
                 |tour_complete: false
                 |type:
                 |v1_usage_plan_id: 1
                 |v1_support_plan_id: 1
                 |trial_ends_at:
                 |converted_to_v2_at:
                 |v2_usage_plan_id:
                 |v2_support_plan_id:
                 |state:
                 |invoice_type:
                 |developer:
                 |company_industry:
                 |address_zip:
                 |company_type:
                 |job_type:
                 |qs_delivery:
                 |role:
                 |id: 90137
               """.stripMargin
    val u = udf.Deserializer.$deserialize(mess).get

    u should contain theSameElementsInOrderAs Array(null, null, null, null, null, null,
      "i5U4AvtSv1RW2MkKxC7q", null, "xword", null, "''", null, "foo@bar.com", null, null,
      "chin", null, null, null, 1, true, null, null, 90137, null, null, null, "2012-10-24 10:29:55.228935000 Z",
      0, null, "2012-10-24 10:29:55.180866000 Z", null, false, null, null, null, "2015-01-09 06:39:14.200373835 Z",
      null, null, "XmpAAp11iydxH52a6MCB", null, null, "''", 1)
  }
}