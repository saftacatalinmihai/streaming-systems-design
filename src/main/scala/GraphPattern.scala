import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge}
import akka.{Done, NotUsed}

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future}

object GraphPattern {

  /**
   * bypass takes a flow and a function from the flow's input to it's output as Option.
   * If the function returns Some, the inner flow is bypassed and the output is directly emitted.
   * {{{
   *       +------------------------------+
   *  I ~> | F ~> B ~> throughInner ~> M  | ~> O
   *       |      B ~> bypass       ~> M  |
   *       +------------------------------+
   * }}}
   */
  def bypass[I, O](
                    inner: FlowShape[I, O]
                  )(
                    bypassFunction: I => Option[O]
                  )(
                    implicit builder: GraphDSL.Builder[NotUsed]
                  ): FlowShape[I, O] = {
    import GraphDSL.Implicits._

    val F = builder.add(Flow[I].map { i => (i, bypassFunction(i)) })
    val B = builder.add(Broadcast[(I, Option[O])](2))

    val bypass = builder.add(Flow[(I, Option[O])].collect {
      case (_, Some(o)) =>
        println(s"bypassing")
        o
    })

    val _through = builder.add(Flow[(I, Option[O])].collect {
      case (i, None) => i
    })

    _through ~> inner
    val throughInner = FlowShape(_through.in, inner.out)

    val M = builder.add(Merge[O](2))

    // format: off
    F ~> B ~> throughInner ~> M
    B ~> bypass ~> M
    // format: on

    new FlowShape(F.in, M.out)
  }

  final case class RawInput(raw: String) {
    def toUser(userId: String): User = User(userId, raw)
  }

  final case class User(raw: String, userId: String, userPermissions: Option[List[String]] = None, newsletter: Option[String] = None, sent: Boolean = false)

  final case class UserPermissions(raw: String, userId: String, userPermissions: List[String], newsletter: Option[String] = None, sent: Boolean = false)

  final case class UserNewsletter(raw: String, userId: String, userPermissions: List[String], newsletter: String, sent: Boolean)

  def parse(raw: RawInput): User = {
    println(s"Parsing $raw")
    val userId = raw.raw.split("userId:")(1)
    User(raw.raw, userId)
  }

  def getPermissions(user: User): UserPermissions = {
    println(s"getPermissions for user: $user")
    val userPermissions = List("permission1", "permission2")
    UserPermissions(user.raw, user.userId, userPermissions)
  }

  def getNewsletter(userPermissions: UserPermissions): UserNewsletter = {
    println(s"getNewsletter for user: $userPermissions")
    val newsletter = "newsletter1"
    UserNewsletter(userPermissions.raw, userPermissions.userId, userPermissions.userPermissions, newsletter, sent = false)
  }

  def sendNewsletter(userNewsletter: UserNewsletter): UserNewsletter = {
    println(s"Sending newsletter to user ${userNewsletter.userId}")
    userNewsletter
  }

  val repository: Repository = Repository(Map.empty)

  final case class T1(raw: RawInput)

  final case class T2(raw: String, user: User, userPermissions: Option[UserPermissions] = None, userNewsletter: Option[String] = None, sent: Boolean = false)

  final case class T3(raw: String, userId: String, userPermissions: List[String], newsletter: Option[String] = None, sent: Boolean = false)

  final case class T4(raw: String, userId: String, userPermissions: List[String], newsletter: String, sent: Boolean)

  val parseFlow: Flow[RawInput, User, NotUsed] =
    Flow[RawInput].map(parse)

  val getPermissionsFlow: Flow[User, UserPermissions, NotUsed] =
    Flow[User].map(getPermissions).map { up => repository.savePermissions(up.userId, up.userPermissions); up }

  val getNewsletterFlow: Flow[UserPermissions, UserNewsletter, NotUsed] =
    Flow[UserPermissions].map(getNewsletter).map { n => repository.saveNewsletter(n.userId, n.newsletter); n }

  val sendNewsletterFlow: Flow[UserNewsletter, UserNewsletter, NotUsed] =
    Flow[UserNewsletter].map(sendNewsletter).map { n => repository.saveNewsletterSent(n.userId, sent = true); n }

  final case class Repository(var db: Map[String, Any]) {
    def save(user: User): (Option[User], Option[List[String]], Option[String], Boolean) = {
      getState(user.userId) match {
        case (None, None, None, false) =>
          println(s"Saving received user: $user")
          db = db + (user.userId -> user)
          (None, None, None, false)

        case (user, userPermissions, userNewsletter, newsletterSent) =>
          (user, userPermissions, userNewsletter, newsletterSent)
      }
    }

    def savePermissions(userId: String, userPermissions: List[String]): Unit = {
      println(s"Saving permissions for user: $userPermissions")
      db = db + (userId + "_permissions" -> userPermissions)
    }

    def saveNewsletter(userId: String, userNewsletter: String): Unit = {
      println(s"Saving newsletter for user: $userNewsletter")
      db = db + (userId + "_newsletter" -> userNewsletter)
    }

    def saveNewsletterSent(userId: String, sent: Boolean): Unit = {
      println(s"Saving newsletter sent for user: $sent")
      db = db + (userId + "_newsletter_sent" -> sent)
    }

    def getState(userId: String): (Option[User], Option[List[String]], Option[String], Boolean) = {
      val user = db.get(userId).map(_.asInstanceOf[User])
      val userPermissions = db.get(userId + "_permissions").map(_.asInstanceOf[List[String]])
      val userNewsletter = db.get(userId + "_newsletter").map(_.asInstanceOf[String])
      val userNewsletterSent = db.get(userId + "_newsletter_sent").exists(_.asInstanceOf[Boolean])
      (user, userPermissions, userNewsletter, userNewsletterSent)
    }
  }

  def main(args: Array[String]): Unit = {
    import akka.actor.ActorSystem
    import akka.stream.scaladsl._

    implicit val system: ActorSystem = ActorSystem("GraphPattern")


    val flow = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val parse = builder.add(parseFlow)

      val save = builder.add(Flow[User].map { user =>
        val (_, permissions, newsletter, newsletterSent) = repository.save(user)
        user.copy(userPermissions = permissions, newsletter = newsletter, sent = newsletterSent)
      })

      val getPermissions = bypass(builder.add(getPermissionsFlow))(user => user.userPermissions.map(p => UserPermissions(user.raw, user.userId, p, user.newsletter, user.sent)))
      val getNewsletter = bypass(builder.add(getNewsletterFlow))(userPermissions => userPermissions.newsletter.map(n => UserNewsletter(userPermissions.raw, userPermissions.userId, userPermissions.userPermissions, n, userPermissions.sent)))
      val sendNewsletter = bypass(builder.add(sendNewsletterFlow))(userNewsletter => if (userNewsletter.sent) Some(userNewsletter) else None)

      parse ~> save ~> getPermissions ~> getNewsletter ~> sendNewsletter

      FlowShape(parse.in, sendNewsletter.out)
    })

    val done: Future[Done] = Source.tick(0.seconds, 3.seconds, RawInput("userId:123"))
      .via(flow)
      .runForeach(println)

    Await.result(done, Duration.Inf)
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
