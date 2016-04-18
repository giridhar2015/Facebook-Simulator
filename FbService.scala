import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.actor._
import akka.io._
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import spray.can.server.Stats
import spray.http._
import spray.http.MediaTypes._
import spray.httpx.SprayJsonSupport
import spray.httpx.marshalling.Marshaller
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing._


sealed trait serviceMessage
case class createUsers(count:Int) extends serviceMessage
case class addFriends(frndLst:ArrayBuffer[String]) extends serviceMessage
case class getFriendList(ctx:RequestContext) extends serviceMessage
case class getProfile(ctx:RequestContext) extends serviceMessage
case class createProfile(name:String,age:String,gender:String) extends serviceMessage
case class friendListResponse(frndLst:Set[String]) extends serviceMessage
case class addFriend(frndName:String,frndActr:ActorRef) extends serviceMessage
case class postMessage(user2:String, message:String,timeStamp:String,ctx:RequestContext) extends serviceMessage
case class getPage(ctx:RequestContext) extends serviceMessage
case class addPost(to:String,message:String,time:String) extends serviceMessage
case class addFollowers(followers:ArrayBuffer[String],followerMap:Map[String,ActorRef]) extends serviceMessage
case class addFollower(follower:String,followerActor:ActorRef) extends serviceMessage
case class getPageFollowers(ctx:RequestContext) extends serviceMessage
case class addPage(pageName:String,pageRef:ActorRef) extends serviceMessage
case class getPageList(ctx:RequestContext) extends serviceMessage
case class deletePage(ctx:RequestContext) extends serviceMessage
case class deleteUser(ctx:RequestContext) extends serviceMessage
case class removePage(pageName:String) extends serviceMessage
case class removeUser(userName:String) extends serviceMessage

object FbService extends App with SimpleRoutingApp{
  implicit val system = ActorSystem("Facebook")
  val service = system.actorOf(Props[SprayFacebookActor], "SpraySimpleActor")
  IO(Http) ! Http.Bind(service, interface = "localhost", port = 8000)
  println("print a letter to end it")
}
case class createUserReceive(userName:String,age:String,gender:String,friendListSize:Int,userPrefix:String)
case class updateUserReceive(userName:String,friendListSize:Int)
case class friendsResponse(frndList:Set[String])
case class profileResponse(profile:Map[String,String])
case class postReceive(from:String,message:String,typ:String,timeStamp:Long)
case class postResponse(user2:String,message:String,time:String)
case class createPageReceive(pageName:String)
case class pageFollowersResponse(pageFollowerSet:Set[String])
case class pageListResponse(pgSet:Set[String])

object serviceProtocol extends DefaultJsonProtocol with SprayJsonSupport {
    implicit val oJson1 = jsonFormat5(createUserReceive.apply)
    implicit val oJson11 = jsonFormat2(updateUserReceive.apply)
    implicit val oJson2 = jsonFormat1(friendsResponse.apply)
    implicit val oJson3 = jsonFormat1(profileResponse.apply)
    implicit val oJson4 = jsonFormat4(postReceive.apply)
    implicit val oJson5 = jsonFormat3(postResponse.apply)
    implicit val oJson6 = jsonFormat1(createPageReceive.apply)
    implicit val oJson7 = jsonFormat1(pageFollowersResponse.apply)
    implicit val oJson8 = jsonFormat1(pageListResponse.apply)
}

case class pageResponse(postArr:List[postResponse])

object serviceProtocol1 extends DefaultJsonProtocol with SprayJsonSupport{
  import serviceProtocol._
  implicit val ooJson1 = jsonFormat1(pageResponse.apply)
}

class SprayFacebookActor extends Actor with SprayFacebookService{
  def actorRefFactory = context
  def receive = runRoute(sprayFriendListRoute ~ sprayProfileRoute ~ sprayPostRoute ~ sprayStatsRoute ~
                    sprayHomepageRoute ~ sprayCreateUserRoute ~ sprayCreatePageRoute ~ sprayPageRoute ~ sprayPageFollowerRoute ~ 
                    sprayUserPagesRoute ~ sprayDeleteUserRoute ~ sprayDeletePageRoute)
}

trait SprayFacebookService extends HttpService {
  import serviceProtocol._
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val statsMarshaller: Marshaller[Stats] =
    Marshaller.delegate[Stats, String](ContentTypes.`text/plain`) {
      statistics =>
        "Facebook Uptime in nanoseconds   : " + statistics.uptime + '\n' +
        "Number of total requests until now        : " + statistics.totalRequests + '\n' +
        "Number of open requests right now        : " + statistics.openRequests + '\n' +
        "Number of max open requests at an instance    : " + statistics.maxOpenRequests + '\n' +
        "Number of total connections used    : " + statistics.totalConnections + '\n' +
        "Number of open connections right now     : " + statistics.openConnections + '\n' +
        "Max number of open connections at an instance : " + statistics.maxOpenConnections + '\n' +
        "Number of requests timed out    : " + statistics.requestTimeouts + '\n'
    }
  
  val fbUsers:ArrayBuffer[String] = new ArrayBuffer[String]
  val userMap = scala.collection.mutable.Map[String,ActorRef]()
  val fbPages:ArrayBuffer[String] = new ArrayBuffer[String]
  val pageMap = scala.collection.mutable.Map[String,ActorRef]()
  
  def sprayFriendListRoute= {
    path( Segment / "friendlist" ){ userId =>
      get { ctx =>
          if(!userMap.contains(userId)) {
            ctx.complete(userId + " is not registered in facebook")
          }
          else{
            userMap(userId) ! getFriendList(ctx) 
          }
          
          
      }
    }
  }
  def sprayCreateUserRoute = {
    path("createUser"){
      put {
        entity(as[createUserReceive]) { createDetails =>
          if(userMap.contains(createDetails.userName)) {
            complete(createDetails.userName + " is already registered in facebook")
          }
          else{
            val result = createUser(createDetails.userName,createDetails.age,createDetails.gender,createDetails.friendListSize,createDetails.userPrefix)
            complete(result)
          }
          
        }
        
      }
    }
  }
  def sprayCreatePageRoute = {
    path("createPage"){
      put {
        entity(as[createPageReceive]) { createDetails =>
          if(userMap.contains(createDetails.pageName)) {
            complete(createDetails.pageName + " is already registered in facebook")
          }
          else{
            val result = createPage(createDetails.pageName)
            complete(result)
          }
          
        }
        
      }
    }
  }
  
  def sprayPostRoute = {
    path(Segment / "post"){ to =>
      post{ 
          entity(as[postReceive]){ postDetails => ctx =>
            if(postDetails.typ.equals("USER")){
              if(!userMap.contains(to)) {
                ctx.complete(to + " is not registered in facebook") 
              }
              else{
                userMap(to) ! postMessage(postDetails.from,postDetails.message,postDetails.timeStamp.toString(),ctx)
              }
            }
            else if(postDetails.typ.equals("PAGE")){
              if(!pageMap.contains(to)) {
                ctx.complete(to + " is not registered in facebook") 
              }
              else{
                pageMap(to) ! postMessage(postDetails.from,postDetails.message,postDetails.timeStamp.toString(),ctx)
              }
              
            }
            else{
              ctx.complete("Incorrect post type")
            }  
          }
        //}
        
      }
      
    }
  }
  def sprayProfileRoute = {
    path(Segment / "profile"){ userId =>
      get { ctx =>
          if(!userMap.contains(userId)) {
              ctx.complete(userId + " is not registered in facebook")
             
          }
          else{
            userMap(userId) ! getProfile(ctx)  
          }
          
      }
    }
  }
  def sprayUserPagesRoute = {
    path(Segment / "pages"){ userId =>
      get { ctx =>
          if(!userMap.contains(userId)) {
              ctx.complete(userId + " is not registered in facebook")
             
          }
          else{
            userMap(userId) ! getPageList(ctx)  
          }
          
      }
    }
  }
  def sprayHomepageRoute = {
    path(Segment / "homepage"){ userId =>
      get { ctx =>
          if(!userMap.contains(userId)) {
            import serviceProtocol1._
            ctx.complete(userId + " is not registered in facebook")
          }
          else{
            userMap(userId) ! getPage(ctx)  
          }
          
      }
    }
  }
  def sprayStatsRoute = path("stats") {
    get {
      complete {
        actorRefFactory.actorSelection("/user/IO-HTTP/listener-0")
          .ask(Http.GetStats)(1.second)
          .mapTo[Stats]
      }
    }
  }
  
  def sprayPageFollowerRoute = {
    path("page" / Segment / "followers"){ pageId =>
      get { ctx =>
          if(!pageMap.contains(pageId)) {
            ctx.complete(pageId + " is not registered in facebook")
          }
          else{
            pageMap(pageId) ! getPageFollowers(ctx) 
          }
      }
    }
  }
  
  def sprayPageRoute = {
    path("page" / Segment){ pageId =>
      get { ctx =>
          
          if(!pageMap.contains(pageId)) {
            import serviceProtocol1._
            ctx.complete(pageId + " is not registered in facebook")
          }
          else{
            pageMap(pageId) ! getPage(ctx)
          }
          
      }
    }
  }
  
  def sprayDeletePageRoute = {
    path("pagedelete" / Segment){ pageId =>
      delete { ctx =>
          
          if(!pageMap.contains(pageId)) {
            import serviceProtocol1._
            ctx.complete(pageId + " is not registered in facebook")
          }
          else{
            pageMap(pageId) ! deletePage(ctx)
            fbPages -= pageId
            pageMap -= pageId
          }
          
      }
    }
  }
  
  def sprayDeleteUserRoute = {
    path("userdelete" / Segment){ userId =>
      delete { ctx =>
          
          if(!userMap.contains(userId)) {
            ctx.complete(userId + " is not registered in facebook")
          }
          else{
            userMap(userId) ! deleteUser(ctx)
            fbUsers -= userId
            userMap -= userId
          }
          
      }
    }
  }
  
  def createUser(name:String,Age:String,Gender:String,friendListSize:Int,userPrefix:String) : String = {
    
    var listSize = friendListSize
    if(userMap.contains(name)) return name + " is already registered in facebook"
    var userRef = actorRefFactory.actorOf(Props(new userActor(name)), name)
    fbUsers += name
    userMap += (name -> userRef)
    userRef ! createProfile(name,Age,Gender)
    var rn = 0;
    var isDone = false
    var rnUser = ""
    
    var tempArray = new ArrayBuffer[String]
    //var i = 0
    for(i <- 0 until (1*listSize)){
      var counter = 0
      //while(!isDone){
        rn = Random.nextInt(fbUsers.length)
        rnUser = userPrefix+rn
        if(userMap.contains(rnUser) && !name.equals(rnUser) && !tempArray.contains(rnUser)){
          tempArray += rnUser
          userRef ! addFriend(rnUser,userMap(rnUser))
          userMap(rnUser) ! addFriend(name,userRef)
          isDone = true
        }
        /*else if(tempArray.contains(rnUser)){
          listSize += 1
        }*/
        counter += 1
        if(counter == 1000) isDone = true
        
      //}
      //isDone = false
    }
    return "Facebook user " + name + " is created"
  }
  
  def createPage(name:String) : String = {
    if(pageMap.contains(name)) return name + " is already registered in facebook"
    var pageRef = actorRefFactory.actorOf(Props(new pageActor(name)), name)
    fbPages += name
    pageMap += (name -> pageRef)
     //println("got request for " + name + " pageMap size " + pageMap.size)
    var followers = new ArrayBuffer[String]()
    var followerMap = Map[String,ActorRef]()
    if(!fbUsers.isEmpty){
      var rn = Random.nextInt(3*fbUsers.length/4)
      if(rn == (3*fbUsers.length/4)){
        rn = rn-1
      }
      for(i <- 0 until rn){
        var kn = Random.nextInt(fbUsers.length)
        if(kn == fbUsers.length){
          kn = kn-1
        }
        var user = fbUsers(kn)
        userMap(user) ! addPage(name,pageRef)
        followers += user
        followerMap += (user -> userMap(user))
      }
      pageRef ! addFollowers(followers,followerMap)
    }
    
    return "Facebook page " + name + " is created"
  }
  
  
}

class pageActor(name:String) extends Actor{
  import serviceProtocol._
  
  val pageName = name
  var pageFollowers = new ArrayBuffer[String] 
  var postArray = new ArrayBuffer[postResponse]
  var pageFollowerMap = Map[String,ActorRef]()
  
  
  def receive = {
    case addFollowers(followers:ArrayBuffer[String],followerMap:Map[String,ActorRef]) =>{
      pageFollowers ++= followers
      pageFollowerMap ++= followerMap
    }
    
    case addPost(from:String,message:String,time:String) => {
       postArray += postResponse(from,message,time) 
    }
    
    case addFollower(follower:String,followerActor:ActorRef) =>{
      pageFollowers += follower
      pageFollowerMap += (follower -> followerActor)
    }
    
    case getPage(ctx:RequestContext) => {
      import serviceProtocol1._
      val b = pageResponse(postArray.toList)
      ctx.complete(b)
    }
    
    case getPageFollowers(ctx:RequestContext) => {
      ctx.complete(pageFollowersResponse(pageFollowers.toSet))
    }
    
    case postMessage(from:String,message:String,timeStamp:String,ctx:RequestContext) => {
      var fromUser = from
      if(fromUser.isEmpty() || fromUser.equals("")){
        if(pageFollowers.isEmpty || pageFollowers.length == 0){
          ctx.complete("No follower to post on page: " + pageName)
        }
        else{
          var rn = Random.nextInt(pageFollowers.length)
          if(rn == pageFollowers.length) fromUser = pageFollowers(rn-1)
          else    fromUser = pageFollowers(rn)
          println(fromUser + " posting on " + pageName)
          pageFollowerMap(fromUser) ! addPost(pageName,message,timeStamp)
          postArray += postResponse(fromUser,message,timeStamp)
          ctx.complete("posting done on page: " + pageName)
        }
      }
      else{
        if(pageFollowers.contains(fromUser)){
          println("* " + fromUser + "posting on " + pageName)
          pageFollowerMap(fromUser) ! addPost(pageName,message,timeStamp)
          postArray += postResponse(fromUser,message,timeStamp)
          ctx.complete("posting done on page: " + pageName)  
        }
        else{
          ctx.complete(fromUser + " is not a follower of " + pageName)
        }
      }
      
    }
    
    case deletePage(ctx:RequestContext) => {
      for(i <- 0 until pageFollowers.length){
        pageFollowerMap(pageFollowers(i)) ! removePage(pageName)
      }
      ctx.complete("deleted the page " + pageName)
      self ! Kill
      
    }
    
    case removeUser(usrName:String) => {
      pageFollowers -= usrName
      pageFollowerMap -= usrName
    }
    
  }
}

class userActor(name:String) extends Actor {
  import serviceProtocol._
  
  val userName = name
  var friendList = new ArrayBuffer[String] 
  var profile = Map[String,String]()
  var myPostArray = new ArrayBuffer[postResponse]
  var toPostArray = new ArrayBuffer[postResponse]
  var friendMap = Map[String,ActorRef]()
  var pageList = new ArrayBuffer[String]
  var pageMap = Map[String,ActorRef]()
  
  def receive = {
    case addFriends(frndLst: ArrayBuffer[String]) => {
      friendList ++= frndLst
    }
    
    case addFriend(frndName:String,frndActr:ActorRef) => {
      if(!friendList.contains(frndName)){
        friendList += frndName
        friendMap += (frndName -> frndActr)
      } 
      
    }
      
    case getFriendList(ctx:RequestContext) => {
      ctx.complete(friendsResponse(friendList.toSet))
    }
    
   case createProfile(name:String,age:String,gender:String) => {
      profile += ("userName" -> name)
      profile += ("Gender" -> gender)
      profile += ("Age" -> age)
      profile += ("Email" -> (name + "@facebook.com"))
      profile += ("MobileNumber" -> "99999")
    }
    
    case getProfile(ctx:RequestContext) => {
      ctx.complete(profileResponse(profile))
    }
    
    case postMessage(from:String,message:String,timeStamp:String,ctx:RequestContext) => {
      var fromUser = from
      if(fromUser.isEmpty() || fromUser.equals("")){
        if(friendList.isEmpty || friendList.length == 0){
          ctx.complete("No friends to post")
        }
        else{
          var rn = Random.nextInt(friendList.length)
          if(rn == friendList.length) fromUser = friendList(rn-1)
          else  fromUser = friendList(rn)
          println("posting from " + fromUser + " to " + userName)
          friendMap(fromUser) ! addPost(userName,message,timeStamp)
          myPostArray += postResponse(fromUser,message,timeStamp)
          ctx.complete("posting done")
        }
      }
      else{
        if(friendList.contains(fromUser)){
          println("*posting from " + fromUser + " to " + userName)
          friendMap(fromUser) ! addPost(userName,message,timeStamp)
          myPostArray += postResponse(fromUser,message,timeStamp)
          ctx.complete("posting done")  
        }
        else{
          ctx.complete(fromUser + " is not a friend of " + userName)
        }
      }
      
    }
    
    case addPost(to:String,message:String,time:String) => {
      toPostArray += postResponse(to,message,time)
    }
    
    case addPage(pageName:String,pageRef:ActorRef) => {
      pageList += pageName
      pageMap += (pageName -> pageRef)
    }
    
    case getPageList(ctx:RequestContext) => {
      ctx.complete(pageListResponse(pageList.toSet))
    }
    
    case getPage(ctx:RequestContext) => {
      import serviceProtocol1._
      val b = pageResponse(myPostArray.toList)
      ctx.complete(b)
    }
    
    case removePage(pagName:String) => {
      pageList -= pagName
      pageMap -= pagName
    }
    
    case removeUser(usrName:String) => {
      friendList -= usrName
      friendMap -= usrName
    }
    
    case deleteUser(ctx:RequestContext) => {
      for(i <- 0 until friendList.length){
        friendMap(friendList(i)) ! removeUser(userName)
      }
      for(j <- 0 until pageList.length){
        pageMap(pageList(j)) ! removeUser(userName)
      }
      ctx.complete("deleted the user " + userName)
      self ! Kill
    }
      
  }
}

