
package simulator

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import akka.actor._
import akka.io._
import akka.util.Timeout
import spray.client.pipelining._
import spray.http._
import spray.http.HttpCharsets._
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.httpx.SprayJsonSupport._
import spray.httpx.SprayJsonSupport
import spray.httpx.unmarshalling._
import spray.httpx._
import spray.json.DefaultJsonProtocol
import createProtocol.sprayJsonMarshaller
import createProtocol.sprayJsonUnmarshaller
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


sealed trait message
case class getProfile(userId:String) extends message

  case class createUserRequest(userName:String,age:String,gender:String,friendListSize:Int,userPrefix:String)
  case class createPageRequest(pageName:String)
  case class friendsResponse(frndList:Set[String])
  case class postUserRequest(from:String,message:String,typ:String,timeStamp:Long)
  case class postPageRequest(from:String,message:String,typ:String,timeStamp:Long)
  object createProtocol extends DefaultJsonProtocol with SprayJsonSupport{
    implicit val createUserFormat = jsonFormat5(createUserRequest.apply)
    implicit val createPageFormat = jsonFormat1(createPageRequest.apply)
    implicit val friends = jsonFormat1(friendsResponse.apply)
    implicit val postU = jsonFormat4(postUserRequest.apply)
    implicit val postP = jsonFormat4(postPageRequest.apply)
  }

object Simulator extends App {
  import createProtocol._
  println("Simulator started")
  implicit val simSystem = ActorSystem("Simulator")
  import simSystem.dispatcher
  
  var serviceWard = new serviceCoordinator()
  generateUsers(10000)
  startPost()
  
  
  
  def getFriendList(userName:String) = {
    //implicit val timeout = Timeout(5000)
    
    val pipeline: HttpRequest => Future[friendsResponse] = sendReceive ~> unmarshal[friendsResponse]
    val response: Future[friendsResponse] = pipeline(Get("http://localhost:8000/"+userName+"/friendlist"))
    
    response.onComplete {
          case Success(robot) => {
        
            println("Friendlist for " + userName + " is: " + robot.frndList)
            //return robot.frndList.toSet
          }
          case Failure(ex) => {
            println("request is failed " + ex)
            //return null
          }
    }
  }
  
  def startPost() = {
    var state = true
    var counter = 0
    while(state){
      Thread.sleep(10)
      
      var user1 = serviceWard.activeUsers()
      implicit val timeout = Timeout(500000)
      if(user1.equals("") || user1.isEmpty()){
        println("Users are not present in the system")
      }
      else{
        val  userPostStartTime = System.currentTimeMillis()
        var toPost = postUserRequest("","hi","USER",userPostStartTime)
        val pipeline: HttpRequest => Future[String] = sendReceive ~> unmarshal[String]
        val p: Future[String] = pipeline(Post("http://localhost:8000/"+user1 + "/post", toPost))
        p.onComplete { 
          case Success(re) => {
            println("time taken for user post: " + (System.currentTimeMillis() - userPostStartTime))
            //if(counter % 4 == 0){
            for(i <- 0 until 3){
              val  pagePostStartTime = System.currentTimeMillis()
              var page1 = serviceWard.hotPage()
              println("posting on " + page1)
              var toPagePost = postPageRequest("","hi","PAGE",pagePostStartTime)
              val pipeline: HttpRequest => Future[String] = sendReceive ~> unmarshal[String]
              val f: Future[String] = pipeline(Post("http://localhost:8000/"+page1 + "/post", toPagePost))
              f.onComplete{
                case Success(res) => {
                  println("time taken for page post: " + (System.currentTimeMillis() - pagePostStartTime))
                }
                case Failure(ex) => {
                  println("posting on page error " + ex)
                }
              }
            }
          }
          case Failure(ex) => {
            println("Error occurred while posting a message" + ex)
          }
        }
        counter += 1
        if(counter == 1000000){
          state = false
        }
      }
    }
  }
  
  def generateUsers(count:Int) = {
    println("creating users now...")
    val createUserStartTime = System.currentTimeMillis()
    var frCount = 0
    var userPrefix = "user"
    var pagePrefix = "page"
    var pageCount = count/500
    for(i <- 0 until count){
      serviceWard.generateUserInfo(userPrefix,i)
    }
    for(k <- 0 until pageCount){
      serviceWard.fbPages += pagePrefix+k
    }
    for(j <- 0 until count){
      var currentUser = serviceWard.fbUsers(j)
      var userToCreate = createUserRequest(currentUser.userName,currentUser.age,currentUser.gender,currentUser.friendListSize,userPrefix)
      //println("creating users")
      implicit val timeout = Timeout(115000)
      val pipeline: HttpRequest => Future[String] = sendReceive ~> unmarshal[String]
      val f: Future[String] = pipeline(Put("http://localhost:8000/createUser", userToCreate))
      f.onComplete { 
        case Success(ret) => {
          println("time taken for create Users " + frCount + " is " + (System.currentTimeMillis() - createUserStartTime))
          if(frCount == count-1)  println("time taken for create all Users: " + (System.currentTimeMillis() - createUserStartTime))
          if((frCount % (count/pageCount)) == 0){
            val createPageStartTime = System.currentTimeMillis()
            var currentPage = serviceWard.fbPages(frCount / (count/pageCount))
            var pageToCreate = createPageRequest(currentPage)
            println("creating page " + currentPage)
            val pipeline: HttpRequest => Future[String] = sendReceive ~> unmarshal[String]
            val p: Future[String] = pipeline(Put("http://localhost:8000/createPage", pageToCreate))
            f.onComplete { 
              case Success(res) => {
                println("time taken for create Page: " + currentPage + " is "+ (System.currentTimeMillis() - createPageStartTime))
              }
              case Failure(ex) => {
                println("create Page error: " + ex)
              }
            }
          }
          frCount += 1
        }
        case Failure(ex) => {
          println("create User error: " + ex)
        }
      }
    }
  }  
  
    
}


class serviceCoordinator() {
    var fbUsers = new ArrayBuffer[user]
    var fbPages = new ArrayBuffer[String]
    var genderRatio: Double = 0.0
    var fCount: Double = 0
    var chronGp  = new ArrayBuffer[Chron]
    var mCount: Double = 0
    
    chronGp += new Chron(18, 29, 0.19, 15)
    chronGp += new Chron(30, 49, 0.36, 30)
    chronGp += new Chron(50, 65, 0.30, 20)
    chronGp += new Chron(65, 100, 0.15, 22)
    
    
    def generateUserInfo(userPrefix:String,userNumber:Int) = {
      var tmpUser = new user(userPrefix + userNumber)
      setGender(tmpUser)
      setAge(tmpUser)
      tmpUser.mobile += userNumber
      fbUsers += tmpUser
    }
    
    def setAge(tmpUser:user) = {
        var minorChorns = new ArrayBuffer[Chron]
        var currentChron : Chron = null
        var isMinor = true
        var rn = 0
        var minorChrons = new ArrayBuffer[Chron]
        
        for(chron <- chronGp){
          if(chron.count == 0 || ((chron.count / fbUsers.length) < chron.ratio ) ||fbUsers.length == 0 ){
            minorChrons += chron
          }
        }
        if(minorChrons.length > 0){
          currentChron = minorChrons(Random.nextInt(minorChrons.length-1))
        }
        else{
          currentChron = chronGp(Random.nextInt(chronGp.length-1))
        }
        currentChron.count += 1
        currentChron.chronMembers += tmpUser
        tmpUser.friendListSize = currentChron.getListSize()
        tmpUser.age = "" + (currentChron.start + Random.nextInt(currentChron.end - currentChron.start))
      }
    
    def activeUsers() : String = {
      if(fbUsers.length == 0) return ""
       var rn =  Random.nextInt(fbUsers.length)
       if(rn == fbUsers.length) rn = rn-1
       return fbUsers(rn).userName
    }
    
    def hotPage() : String = {
      if(fbPages.length == 0) return ""
       var rn =  Random.nextInt(fbPages.length)
       if(rn == fbPages.length) rn = rn-1
       return fbPages(rn)
    }
    
    def setGender(tmpUser:user) = {
        var gen = ""
        if (mCount >= 1 && fCount < 1) {
          fCount += 1
          gen= "Female"
        }
        else if (fCount >= 1 && mCount < 1) {
          mCount += 1
          gen = "Male"
        }
        else if ((mCount / fbUsers.length) > 0.65) {
          fCount += 1
          gen = "Female"
        }
        else{
          mCount += 1
          gen = "Male"
        }
        tmpUser.gender = gen
    }
    
}

class user(name:String) {
  var userName = name
  var gender = ""
  var age = "0"
  var email = userName+"@facebook.com"
  var mobile = 9000
  var friendListSize = 0
  
}

class Chron (astart: Int, aend: Int, aratio: Double, alimit : Int){
  var threshold = 2*alimit
  var count = 0
  var mean  = 0.0
  var start = astart
  var end = aend
  var ratio = aratio
  var limit = alimit
  var chronMembers = new ArrayBuffer[user]
  
  def getListSize():Int = {
    var temp = 0
    if(mean < limit){
      temp = (Math.ceil(mean)).toInt + Random.nextInt(threshold-((Math.ceil(mean)).toInt))
    }
    else{
      temp = Random.nextInt((Math.ceil(mean)).toInt)
    }
    mean = ((mean*(count-1).toDouble + temp.toDouble)/count.toDouble)
    return temp
  }
  
  
}




